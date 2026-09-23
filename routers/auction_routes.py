from fastapi import APIRouter, HTTPException, Request
from datetime import datetime, timedelta, timezone
import pymysql
import asyncio
from decimal import Decimal
from auction.auction_engine import background_timer, stop_timer_task, auction_expiry
from core.database import get_db_connection
from auth.auth_handler import verify_token, get_token_from_request
from sockets.socket_manager import sio, team_sockets
from models.schemas import StartAuctionRequest

router = APIRouter()

@router.post("/start-auction")
async def start_auction(data: StartAuctionRequest, request: Request):

    token = get_token_from_request(request)
    
    if not token:
        raise HTTPException(status_code=401, detail="Unauthorized")

    payload = verify_token(token)

    if not payload or payload.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")

    mode = data.mode
    duration = data.duration or 40
    player_id = data.player_id

    conn = get_db_connection()

    if conn is None:
        raise HTTPException(500, "Database connection failed")

    cursor = conn.cursor(pymysql.cursors.DictCursor)

    try:

        # 🚫 prevent double auction
        cursor.execute("SELECT * FROM current_auction LIMIT 1")
        if cursor.fetchone():
            raise HTTPException(400, "Auction already running")

        # -------- SELECT PLAYER --------

        if mode == "manual":

            if not player_id:
                raise HTTPException(400, "player_id required")

            cursor.execute(
                "SELECT * FROM players WHERE id=%s",
                (player_id,)
            )

            player = cursor.fetchone()

        elif mode == "random":

            cursor.execute("""
                SELECT * FROM players
                WHERE id NOT IN (
                    SELECT player_id FROM sold_players
                    UNION
                    SELECT player_id FROM unsold_players
                )
                ORDER BY RANDOM()
                LIMIT 1
            """)

            player = cursor.fetchone()

        elif mode == "unsold":

            cursor.execute("""
                SELECT p.*
                FROM players p
                JOIN unsold_players u ON p.id = u.player_id
                WHERE p.id NOT IN (
                    SELECT player_id FROM sold_players
                )
                ORDER BY u.id ASC
                LIMIT 1
            """)

            player = cursor.fetchone()

        else:
            raise HTTPException(400, "Invalid mode")

        if not player:
            print("🏁 No eligible players remaining")
            await sio.emit("auction_finished", {
                "message": "No players available for auction"
            })

            return{
                "status": "finished",
                "message": "No players available for auction"
            }

        player_id = player["id"]

        # -------- RESET TABLES --------

        cursor.execute("DELETE FROM current_auction")
        cursor.execute("DELETE FROM live_bids")

        # -------- INSERT CURRENT AUCTION --------

        start_time = datetime.now(timezone.utc)
        expires_at = start_time + timedelta(seconds=duration)

        cursor.execute("""
            INSERT INTO current_auction
            (player_id, start_time, expires_at, auction_duration, mode)
            VALUES (%s,%s,%s,%s,%s)
        """, (
            player_id,
            start_time,
            expires_at,
            duration,
            mode
        ))

        conn.commit()

        # -------- SOCKET EVENTS --------

        await sio.emit("timer_update", {
            "remaining_seconds": duration,
            "server_time": start_time.isoformat()
        })

        await sio.emit("auction_started", {
            "player": {
                "id": player["id"],
                "name": player["name"],
                "image_path": player["image_path"],
                "jersey": player["jersey"],
                "category": player["category"],
                "type": player["type"],
                "base_price": float(player.get("base_price")or 0),
                "highest_runs": player["highest_runs"]
                },
            "duration": duration,
            "expires_at": expires_at.isoformat(),
            "current_bid": float(player.get("base_price") or 0),
            "history": []
        })

        # -------- START TIMER --------

        asyncio.create_task(
            background_timer(
                player_id,
                mode,
                payload.get("session_id")
            )
        )

        print(f"🚀 Auction started for {player['name']}")

        return {
            "status": "auction_started",
            "player_id": player_id,
            "player_name": player["name"],
            "duration": duration,
            "expires_at": expires_at.isoformat()
        }

    except Exception as e:
        conn.rollback()
        raise e

    except Exception as e:
        conn.rollback()
        print("❌ start-auction error: ",e)
        raise HTTPException(status_code=500, detail="Internal server error")

    finally:
        cursor.close()
        conn.close()


def seconds_remaining(expires_at):
    now = datetime.now(timezone.utc)
    remaining = (expires_at - now).total_seconds()
    return max(0, int(remaining))

@router.get("/current-auction")
async def get_current_auction(request: Request):

    token = get_token_from_request(request)

    if not token:
        raise HTTPException(status_code=401, detail="Unauthorized")

    user = verify_token(token)

    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    try:
        #STEP 1 : Fetch Current auction
        cursor.execute("""
            SELECT 
                ca.player_id,
                ca.start_time,
                ca.expires_at,
                ca.auction_duration,
                ca.paused,
                ca.paused_remaining,
                p.name, p.image_path, p.jersey,
                p.category, p.type, p.base_price,
                p.highest_runs, p.total_runs
            FROM current_auction ca
            JOIN players p ON ca.player_id = p.id
            LIMIT 1
        """)
        auction = cursor.fetchone()

        if not auction:
            return {"status": "no_active_auction"}

        player_id = auction["player_id"]

        base_price = auction["base_price"] or 0

        if isinstance(base_price, Decimal):
            base_price = float(base_price)

        # Remaining time
        expires_at = auction["expires_at"]

        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)

        paused = bool(auction.get("paused"))
        paused_remaining = int(auction.get("paused_remaining") or 0)

        remaining = paused_remaining if paused else seconds_remaining(expires_at)

        # STEP 3: Highest bid
        cursor.execute("""
            SELECT b.team_id, t.name AS team_name, b.bid_amount
            FROM live_bids b
            JOIN teams t ON b.team_id = t.team_id
            WHERE b.player_id = %s
            ORDER BY b.bid_amount DESC, b.bid_time ASC
            LIMIT 1
        """, (player_id,))

        top_bid = cursor.fetchone()

        current_bid = float(top_bid["bid_amount"]) if top_bid else float(base_price)

        # STEP 4: TEAM BALANCE
        team_balance = 0

        if user.get("role") == "team":

            cursor.execute(
                "SELECT purse FROM teams WHERE team_id=%s",
                (user.get("team_id"),)
            )

            team = cursor.fetchone()

            team_balance = float(team["purse"]) if team else 0

        # STEP 5: BID HISTORY
        cursor.execute("""
            SELECT 
                b.team_id,
                t.name AS team_name,
                b.bid_amount,
                b.bid_time
            FROM live_bids b
            JOIN teams t ON b.team_id = t.team_id
            WHERE b.player_id = %s
            ORDER BY b.bid_time ASC
        """, (player_id,))

        history_raw = cursor.fetchall() or []

        history = []

        for row in history_raw:

            bt = row.get("bid_time")

            bid_time_str = (
                bt.strftime("%Y-%m-%d %H:%M:%S") if bt else None
            )

            history.append({
                "team_id": row["team_id"],
                "team_name": row["team_name"],
                "bid_amount": float(row["bid_amount"]),
                "bid_time": bid_time_str,
            })

        return {
            "status": "auction_active",
            "player": {
                "id": auction["player_id"],
                "name": auction["name"],
                "jersey": auction["jersey"],
                "category": auction["category"],
                "type": auction["type"],
                "image_path": auction["image_path"],
                "base_price": float(base_price),
                "highest_runs": auction["highest_runs"],
            },
            "currentBid": current_bid,
            "highest_bid": top_bid,
            "remaining_seconds": remaining,
            "auction_duration": auction["auction_duration"],
            "teamBalance": team_balance,
            "nextSteps": [
                current_bid + 500,
                current_bid + 1000,
                current_bid + 1500
            ],
            "paused": paused,
            "canBid": user.get("role") == "team",
            "history": history
        }

    except Exception as e:
        print("❌ ERROR in /current-auction:", e)
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        cursor.close()
        conn.close()

@router.post("/pause-auction")
async def pause_auction(request: Request):

    # ---------- AUTH CHECK ----------
    token = get_token_from_request(request)

    if not token:
        raise HTTPException(status_code=401, detail="Unauthorized")

    payload = verify_token(token)

    if not payload or payload.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")

    conn = get_db_connection()

    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor(pymysql.cursors.DictCursor)

    try:

        # ---------- GET ACTIVE AUCTION ----------
        cursor.execute("""
            SELECT player_id, expires_at, paused
            FROM current_auction
            LIMIT 1
        """)

        auction = cursor.fetchone()

        if not auction:
            raise HTTPException(status_code=400, detail="No active auction")

        if auction["paused"] == 1:
            raise HTTPException(status_code=400, detail="Auction already paused")

        player_id = auction["player_id"]
        expires_at = auction["expires_at"]

        # ---------- Normalize datetime ----------
        if isinstance(expires_at, str):
            expires_at = datetime.fromisoformat(expires_at)

        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)

        now = datetime.now(timezone.utc)

        remaining = max(0, int((expires_at - now).total_seconds()))

        # ---------- UPDATE DB ----------
        cursor.execute("""
            UPDATE current_auction
            SET paused = true,
                paused_remaining = %s
            WHERE player_id = %s
        """, (remaining, player_id))

        conn.commit()

        print(f"⏸ Auction paused for player {player_id} with {remaining}s remaining")

        # ---------- CANCEL BACKGROUND TIMER ----------
        from auction.auction_engine import stop_timer_task
        stop_timer_task(player_id)

        # ---------- SOCKET EVENT ----------
        await sio.emit("auction_paused", {
            "paused": True,
            "remaining_seconds": remaining
        })

        return {
            "status": "auction_paused",
            "player_id": player_id,
            "remaining_seconds": remaining
        }

    except Exception as e:
        conn.rollback()
        print("❌ Pause auction error:", e)
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        cursor.close()
        conn.close()

@router.post("/resume-auction")
async def resume_auction(request: Request):

    # ------------- AUTH CHECK -------------
    token = get_token_from_request(request)

    if not token:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    payload = verify_token(token)

    if not payload or payload.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")
    
    conn = get_db_connection()

    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    try:

        # -------------- FIND PAUSED AUCTION --------------
        cursor.execute("""
            SELECT player_id, paused_remaining, mode
            FROM current_auction
            WHERE paused = true
            LIMIT 1
        """)

        auction = cursor.fetchone()

        if not auction:
            raise HTTPException(status_code=400, detail="No paused auction found")
        
        remaining = auction["paused_remaining"] or 0

        if remaining <= 0:
            raise HTTPException(status_code=400, detail="Auction time already ended")
        
        # ---------------- NEW EXPIRY ------------------
        new_end_time = datetime.now(timezone.utc) + timedelta(seconds=remaining)

        cursor.execute("""
            UPDATE current_auction
            SET paused = false,
                paused_remaining = NULL,
                expires_at = %s
            WHERE player_id = %s
        """,(new_end_time, auction["player_id"]))

        conn.commit()

        player_id = auction["player_id"]
        mode = auction["mode"]

        print(f"▶ Auction resumed for player {player_id} - {remaining}s remaining")

        # --------------- START TIMER AGAIN -----------------
        from auction.auction_engine import background_timer
        asyncio.create_task(
            background_timer(
                player_id,
                mode,
                payload.get("session_id")
            )
        )

        # ---------------- NOTIFY CLIENTS ----------------
        await sio.emit("auction_resumed",{
            "paused": False,
            "remaining_seconds": remaining,
            "expires_at": new_end_time.isoformat()
        })

        return{
            "message": "Auction resumed successfully",
            "player_id": player_id,
            "remaining": remaining
        }
    
    except Exception as e:
        conn.rollback()
        print("❌ Resume auction error: ", e)
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        cursor.close()
        conn.close()


async def process_next_auction_background(player_id, mode, session_id):
    conn = get_db_connection()
    if not conn:
        print("⚠ DB connection failed in process_next_auction_background")
        return
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    try:
        # 2. Fetch player info
        cursor.execute("""
            SELECT id, name, category, type, image_path, base_price
            FROM players
            WHERE id = %s
        """, (player_id,))
        player_info = cursor.fetchone()
        from decimal import Decimal
        if player_info:
            for k, v in player_info.items():
                if isinstance(v, Decimal):
                    player_info[k] = float(v)
        if not player_info:
            player_info = {"id": player_id, "name": "Unknown"}

        # 3. Check highest bid in live_bids
        cursor.execute("""
            SELECT b.team_id, b.bid_amount, t.name AS team_name, t.image_path
            FROM live_bids b
            JOIN teams t ON b.team_id = t.team_id
            WHERE b.player_id = %s
            ORDER BY b.bid_amount DESC, b.bid_time ASC
            LIMIT 1
        """, (player_id,))
        top_bid = cursor.fetchone()

        if top_bid:
            sold_price = float(top_bid["bid_amount"])
            team_id = top_bid["team_id"]
            team_name = top_bid["team_name"]
            team_image = top_bid.get("image_path")

            cursor.execute("""
                UPDATE teams
                SET purse = GREATEST(0.0, purse - %s),
                    Players_Bought = COALESCE(Players_Bought, 0) + 1
                WHERE team_id = %s
            """, (sold_price, team_id))

            cursor.execute("SELECT purse FROM teams WHERE team_id = %s", (team_id,))
            team_row = cursor.fetchone()
            updated_purse = float(team_row["purse"]) if team_row else 0.0

            winner_sid = team_sockets.get(team_id)
            if winner_sid:
                await sio.emit("purse_update", {"purse": updated_purse}, to=winner_sid)

            cursor.execute("""
                INSERT INTO sold_players (player_id, team_id, sold_price, session_id, sold_time)
                VALUES (%s, %s, %s, %s, NOW())
            """, (player_id, team_id, sold_price, session_id))

            end_payload = {
                "status": "sold",
                "player": player_info,
                "team": {
                    "team_id": team_id,
                    "team_name": team_name,
                    "bid_amount": sold_price,
                    "image_path": team_image
                },
                "sold_price": sold_price,
                "message": f"Player sold to {team_name} for ₹{sold_price}"
            }
            print(f"✅ Next Player forced: Player {player_info.get('name')} SOLD to {team_name} for ₹{sold_price}")
        else:
            cursor.execute("""
                INSERT INTO unsold_players (player_id, reason, session_id, added_on)
                VALUES (%s, %s, %s, NOW())
            """, (player_id, "No Bids", session_id))

            end_payload = {
                "status": "unsold",
                "player": player_info,
                "base_price": player_info.get("base_price"),
                "message": "No bids received — player marked UNSOLD"
            }
            print(f"⚠️ Next Player forced: Player {player_info.get('name')} marked UNSOLD")

        cursor.execute("DELETE FROM current_auction WHERE player_id = %s", (player_id,))
        cursor.execute("DELETE FROM live_bids WHERE player_id = %s", (player_id,))
        conn.commit()

        await sio.emit("auction_ended", end_payload)
        await sio.emit("next_player_loading", {"delay": 10})

        # 4. Advance to next player after delay
        print("⏳ Waiting 10 seconds before next player")
        await asyncio.sleep(10)

        cursor.execute("""
            SELECT * FROM players
            WHERE id NOT IN (
                SELECT player_id FROM sold_players
                UNION
                SELECT player_id FROM unsold_players
            )
            ORDER BY RANDOM()
            LIMIT 1
        """)
        next_player = cursor.fetchone()

        if not next_player:
            print("🏁 Auction finished — all players processed")
            await sio.emit("auction_finished", {"message": "All players processed"})
            return

        start_time = datetime.now(timezone.utc)
        duration = 120
        expires_at = start_time + timedelta(seconds=duration)

        cursor.execute("""
            INSERT INTO current_auction (player_id, start_time, expires_at, auction_duration, mode)
            VALUES (%s, %s, %s, %s, %s)
        """, (next_player["id"], start_time, expires_at, duration, mode))
        conn.commit()

        await sio.emit("auction_started", {
            "player": {
                "id": next_player["id"],
                "name": next_player["name"],
                "image_path": next_player["image_path"],
                "jersey": next_player["jersey"],
                "category": next_player["category"],
                "type": next_player["type"],
                "base_price": float(next_player.get("base_price") or 0),
                "highest_runs": next_player.get("highest_runs") or 0
            },
            "duration": duration,
            "expires_at": expires_at.isoformat(),
            "current_bid": float(next_player.get("base_price") or 0),
            "history": []
        })

        from auction.auction_engine import background_timer
        asyncio.create_task(
            background_timer(
                next_player["id"],
                mode,
                session_id
            )
        )
        print(f"🚀 Next auction started for {next_player['name']}")

    except Exception as e:
        conn.rollback()
        print("❌ Error in process_next_auction_background:", e)
    finally:
        cursor.close()
        conn.close()

@router.post("/next-auction")
async def next_auction(request: Request):

    token = get_token_from_request(request)
    if not token:
        raise HTTPException(status_code=401, detail="Unauthorized")

    payload = verify_token(token)
    if not payload or payload.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor(pymysql.cursors.DictCursor)
    try:
        cursor.execute("SELECT player_id, mode FROM current_auction LIMIT 1")
        auction = cursor.fetchone()

        if not auction:
            raise HTTPException(status_code=400, detail="No active auction")

        player_id = auction["player_id"]
        mode = auction.get("mode", "random")
        session_id = payload.get("session_id", "default")

        # 1. Stop background timer task immediately to prevent concurrent timers
        stop_timer_task(player_id)

        # 2. Spawn background task to handle end of auction and next player
        asyncio.create_task(process_next_auction_background(player_id, mode, session_id))

        return {"success": True, "message": "Next player sequence initiated"}

    except HTTPException:
        raise
    except Exception as e:
        print("❌ Error in next_auction:", e)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@router.post("/cancel-auction")
async def cancel_auction(request: Request):

    # ---------- AUTH CHECK ----------
    token = get_token_from_request(request)

    if not token:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    payload = verify_token(token)

    if not payload or payload.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")
    
    conn = get_db_connection()

    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    try:
        # ----------- CHECK CURRENT AUCTION ------------
        cursor.execute("SELECT * FROM current_auction LIMIT 1")
        auction = cursor.fetchone()

        if not auction:
            raise HTTPException(status_code=400, detail="No active auction")
        
        player_id = auction["player_id"]

        # --------------- FETCH PLAYER INFO ---------------
        cursor.execute("""
            SELECT id, name, category, type, image_path, base_price
            FROM players
            WHERE id = %s
        """, (player_id,))

        player_info = cursor.fetchone()

        if player_info and isinstance(player_info.get("base_price"), Decimal):
            player_info["base_price"] = float(player_info["base_price"])

        # if not player_info:
        #     player_info = {
        #         "id": player_id,
        #         "name": "Unknown"
        #     }

        # -------------- MARK UNSOLD --------------
        cursor.execute("""
            INSERT INTO unsold_players
            (player_id, reason, added_on)
            VALUES (%s, %s, NOW())
        """, (
            player_id,
            "Auction manually cancelled by admin"
        ))

        # ------------- CLEANUP ----------------
        from auction.auction_engine import stop_timer_task
        stop_timer_task(player_id)

        cursor.execute("DELETE FROM current_auction WHERE player_id = %s", (player_id,))

        cursor.execute("DELETE FROM live_bids WHERE player_id = %s", (player_id,))

        conn.commit()

        # ------------ EMIT EVENT --------------
        await sio.emit("auction_ended", {
            "status":"unsold",
            "player": player_info,
            "message": "🛑 Auction cancelled by admin - player marked unsold manually"
        })

        print(f"🛑 Auction cancelled manually for player {player_info.get('name')}")

        return{
            "message": f"Auction cancelled for {player_info.get('name')}",
            "player": player_info
        }
    
    except Exception as e:

        conn.rollback()
        print("❌ cancel-auction error: ", e)
        raise HTTPException(status_code=500, detail= str(e))
    
    finally:
        cursor.close()
        conn.close()


@router.get("/auction-state")
async def auction_state(request: Request):

    token = get_token_from_request(request)

    if not token:
        raise HTTPException(status_code=401, detail="Unauthorized")

    payload = verify_token(token)

    if not payload:
        raise HTTPException(status_code=403, detail="Invalid token")

    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    try:

        # ---------------- CURRENT AUCTION ----------------
        cursor.execute("""
        SELECT 
            ca.player_id,
            ca.start_time,
            ca.expires_at,
            ca.auction_duration,
            ca.paused,
            ca.paused_remaining,
            p.name,
            p.image_path,
            p.jersey,
            p.category,
            p.type,
            p.base_price,
            p.highest_runs,
            p.total_runs
        FROM current_auction ca
        JOIN players p ON ca.player_id = p.id
        LIMIT 1
        """)

        auction = cursor.fetchone()

        if not auction:
            return {
                "status": "no_active_auction"
            }

        player_id = auction["player_id"]

        # ---------------- REMAINING TIME ----------------
        if auction["paused"]:
            remaining = int(auction["paused_remaining"] or 0)
        else:
            expires_at = auction["expires_at"]

            if isinstance(expires_at, str):
                expires_at = datetime.fromisoformat(expires_at)

            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)

            now = datetime.now(timezone.utc)

            remaining = max(
                0,
                int((expires_at - now).total_seconds())
            )

        # ---------------- HIGHEST BID ----------------
        cursor.execute("""
        SELECT b.team_id, t.name AS team_name, b.bid_amount
        FROM live_bids b
        JOIN teams t ON b.team_id = t.team_id
        WHERE b.player_id = %s
        ORDER BY b.bid_amount DESC
        LIMIT 1
        """, (player_id,))

        highest_bid = cursor.fetchone()

        # ---------------- BID HISTORY ----------------
        cursor.execute("""
        SELECT b.team_id, t.name AS team_name, b.bid_amount, b.bid_time
        FROM live_bids b
        JOIN teams t ON b.team_id = t.team_id
        WHERE b.player_id = %s
        ORDER BY b.bid_time ASC
        """, (player_id,))

        history = cursor.fetchall()

        current_bid = (
            float(highest_bid["bid_amount"])
            if highest_bid
            else float(auction["base_price"])
        )

        return {
            "status": "auction_active",
            "player": {
                "id": player_id,
                "name": auction["name"],
                "jersey": auction["jersey"],
                "category": auction["category"],
                "type": auction["type"],
                "image_path": auction["image_path"],
                "base_price": float(auction["base_price"]),
                "highest_runs": auction["highest_runs"],
                "total_runs": auction["total_runs"]
            },
            "current_bid": current_bid,
            "highest_bid": highest_bid,
            "remaining_seconds": remaining,
            "paused": bool(auction["paused"]),
            "history": history
        }

    except Exception as e:

        raise HTTPException(status_code=500, detail=str(e))

    finally:
        cursor.close()
        conn.close()

@router.get("/auction-status")
async def auction_status():

    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    try:

        cursor.execute("SELECT player_id FROM current_auction LIMIT 1")
        auction = cursor.fetchone()

        if auction:
            return {
                "active": True,
                "player_id": auction["player_id"]
            }

        return {
            "active": False
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        cursor.close()
        conn.close()


@router.post("/mark-sold")
async def mark_sold(request: Request):

    # ---------- AUTH CHECK ----------
    token = get_token_from_request(request)

    if not token:
        raise HTTPException(status_code=401, detail="Unauthorized")

    payload = verify_token(token)

    if not payload or payload.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")
    
    data = await request.json()
    player_id = data.get("player_id")
    session_id = payload.get("session_id", "default")

    if not player_id:
        raise HTTPException(status_code=400, detail="player_id required")

    conn = get_db_connection()

    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    try:

        # Stop background timer task immediately to prevent concurrent timers
        stop_timer_task(player_id)

        # ---------- GET HIGHEST BID ----------
        cursor.execute("""
            SELECT b.team_id, b.bid_amount, t.name AS team_name, t.image_path
            FROM live_bids b
            JOIN teams t ON b.team_id = t.team_id
            WHERE b.player_id = %s
            ORDER BY b.bid_amount DESC, b.bid_time ASC
            LIMIT 1
        """, (player_id,))

        top = cursor.fetchone()

        if not top:
            raise HTTPException(status_code=404, detail="No live bids for this player")

        sold_price = float(top["bid_amount"])
        team_id = top["team_id"]
        team_name = top["team_name"]
        team_image = top["image_path"]

        # ---------- DEDUCT TEAM PURSE ----------
        cursor.execute(
            "UPDATE teams SET purse = GREATEST(0.0, purse - %s), Players_Bought = COALESCE(Players_Bought, 0) + 1 WHERE team_id = %s",
            (sold_price, team_id)
        )
        cursor.execute(
            "SELECT purse FROM teams WHERE team_id=%s",
            (team_id,)
        )
        row = cursor.fetchone()
        updated_purse = float(row["purse"])
        winner_sid = team_sockets.get(team_id)

        if winner_sid:
            await sio.emit(
                "purse_update",
                {"purse": updated_purse},
                to= winner_sid
            )

        # ---------- INSERT SOLD PLAYER ----------
        cursor.execute("""
            INSERT INTO sold_players (player_id, team_id, sold_price, session_id, sold_time)
            VALUES (%s, %s, %s, %s, NOW())
        """, (player_id, team_id, sold_price, session_id))

        # ---------- CLEAN AUCTION TABLES ----------
        cursor.execute(
            "DELETE FROM current_auction WHERE player_id = %s",
            (player_id,)
        )

        cursor.execute(
            "DELETE FROM live_bids WHERE player_id = %s",
            (player_id,)
        )

        conn.commit()

        # ---------- FETCH PLAYER INFO ----------
        cursor.execute("""
            SELECT id, name, category, type, image_path, base_price
            FROM players
            WHERE id = %s
        """, (player_id,))

        player_info = cursor.fetchone()

        if player_info:
            for k, v in player_info.items():
                if isinstance(v, Decimal):
                    player_info[k] = float(v)

        if not player_info:
            player_info = {"id": player_id}

        # ---------- SOCKET PAYLOAD ----------
        payload = {
            "status": "sold",
            "player": player_info,
            "team": {
                "team_id": team_id,
                "team_name": team_name,
                "bid_amount": sold_price,
                "image_path": team_image
            },
            "sold_price": sold_price,
            "message": f"Player sold to {team_name} for ₹{sold_price}"
        }

        # ---------- EMIT SOCKET EVENT ----------
        await sio.emit("auction_ended", payload)

        print(f"✅ Player {player_info.get('name')} SOLD to {team_name} for ₹{sold_price}")

        # ---------- START NEXT AUCTION ----------
        await sio.emit("next_player_loading", {"delay": 10})
        
        print("⏳ Waiting 10 seconds before next player")
        await asyncio.sleep(10)
        
        # -------- SELECT NEXT PLAYER --------
        cursor.execute("""
            SELECT * FROM players
            WHERE id NOT IN (
                SELECT player_id FROM sold_players
                UNION
                SELECT player_id FROM unsold_players
            )
            ORDER BY RANDOM()
            LIMIT 1
            """)
        
        next_player = cursor.fetchone()
        
        if not next_player:
            await sio.emit("auction_finished", {})
            return

        start_time = datetime.now(timezone.utc)
        duration = 120
        expires_at = start_time + timedelta(seconds=duration)
        
        cursor.execute("""
            INSERT INTO current_auction
            (player_id,start_time,expires_at,auction_duration,mode)
            VALUES (%s,%s,%s,%s,%s)
            """, (
                next_player["id"],
                start_time,
                expires_at,
                duration,
                "random"
            ))
        
        conn.commit()
        
        await sio.emit("auction_started", {
            "player": {
                "id": next_player["id"],
                "name": next_player["name"],
                "image_path": next_player["image_path"],
                "jersey": next_player["jersey"],
                "category": next_player["category"],
                "type": next_player["type"],
                "base_price": float(next_player.get("base_price") or 0),
                "highest_runs": next_player.get("highest_runs") or 0
            },
            "duration": duration,
            "expires_at": expires_at.isoformat(),
            "current_bid": float(next_player.get("base_price") or 0),
            "history": []
        })
        asyncio.create_task(
            background_timer(
                next_player["id"],
                "random",
                session_id
            )
        )
        print(f"🚀 Next auction started for {next_player['name']}")

        return {
            "success": True,
            "message": "Player marked as SOLD",
            "player": player_info,
            "team": {
                "team_id": team_id,
                "team_name": team_name,
                "bid_amount": sold_price
            }
        }

    except Exception as e:
        conn.rollback()
        print("❌ Error in mark_sold:", e)
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        cursor.close()
        conn.close()



@router.post("/mark-unsold")
async def mark_unsold(request: Request):

    # ---------- AUTH CHECK ----------
    token = get_token_from_request(request)

    if not token:
        raise HTTPException(status_code=401, detail="Unauthorized")

    payload = verify_token(token)

    if not payload or payload.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")

    try:
        body = await request.json()
    except Exception:
        body = {}

    req_player_id = body.get("player_id")
    session_id = payload.get("session_id", "default")

    conn = get_db_connection()

    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor(pymysql.cursors.DictCursor)

    try:

        # ---------- GET CURRENT PLAYER ----------
        cursor.execute("SELECT player_id, mode FROM current_auction LIMIT 1")
        auction = cursor.fetchone()

        if not auction and not req_player_id:
            raise HTTPException(status_code=400, detail="No active auction")

        player_id = req_player_id or auction["player_id"]
        mode = auction.get("mode", "random") if auction else "random"

        # Stop background timer task immediately to prevent concurrent timers
        stop_timer_task(player_id)

        # ---------- FETCH PLAYER INFO ----------
        cursor.execute("""
            SELECT id, name, category, type, image_path, base_price
            FROM players
            WHERE id = %s
        """, (player_id,))

        player_info = cursor.fetchone()

        if player_info:
            for k, v in player_info.items():
                if isinstance(v, Decimal):
                    player_info[k] = float(v)

        if not player_info:
            player_info = {"id": player_id}

        # ---------- INSERT INTO UNSOLD ----------
        cursor.execute("""
            INSERT INTO unsold_players
            (player_id, reason, session_id, added_on)
            VALUES (%s, %s, %s, NOW())
        """, (
            player_id,
            "Marked unsold manually by admin",
            session_id
        ))

        # ---------- CLEANUP ----------
        cursor.execute(
            "DELETE FROM current_auction WHERE player_id=%s",
            (player_id,)
        )

        cursor.execute(
            "DELETE FROM live_bids WHERE player_id=%s",
            (player_id,)
        )

        conn.commit()

        # ---------- SOCKET EVENT ----------
        payload = {
            "status": "unsold",
            "player": player_info,
            "base_price": player_info.get("base_price"),
            "message": "Player marked as UNSOLD"
        }

        await sio.emit("auction_ended", payload)
        await sio.emit("next_player_loading", {"delay": 10})

        print(f"⚠️ Player {player_info.get('name')} marked UNSOLD")

        # Advance to next player
        print("⏳ Waiting 10 seconds before next player")
        await asyncio.sleep(10)

        cursor.execute("""
            SELECT * FROM players
            WHERE id NOT IN (
                SELECT player_id FROM sold_players
                UNION
                SELECT player_id FROM unsold_players
            )
            ORDER BY RANDOM()
            LIMIT 1
        """)
        next_player = cursor.fetchone()

        if not next_player:
            print("🏁 Auction finished — all players processed")
            await sio.emit("auction_finished", {"message": "All players processed"})
            return {
                "success": True,
                "status": "auction_finished",
                "message": "Player marked as UNSOLD, all players processed",
                "player": player_info
            }

        start_time = datetime.now(timezone.utc)
        duration = 120
        expires_at = start_time + timedelta(seconds=duration)

        cursor.execute("""
            INSERT INTO current_auction (player_id, start_time, expires_at, auction_duration, mode)
            VALUES (%s, %s, %s, %s, %s)
        """, (next_player["id"], start_time, expires_at, duration, mode))
        conn.commit()

        await sio.emit("auction_started", {
            "player": {
                "id": next_player["id"],
                "name": next_player["name"],
                "image_path": next_player["image_path"],
                "jersey": next_player["jersey"],
                "category": next_player["category"],
                "type": next_player["type"],
                "base_price": float(next_player.get("base_price") or 0),
                "highest_runs": next_player.get("highest_runs") or 0
            },
            "duration": duration,
            "expires_at": expires_at.isoformat(),
            "current_bid": float(next_player.get("base_price") or 0),
            "history": []
        })

        asyncio.create_task(
            background_timer(
                next_player["id"],
                mode,
                session_id
            )
        )
        print(f"🚀 Next auction started for {next_player['name']}")

        return {
            "success": True,
            "message": "Player marked as UNSOLD",
            "player": player_info,
            "next_player": {
                "id": next_player["id"],
                "name": next_player["name"],
                "expires_at": expires_at.isoformat()
            }
        }

    except Exception as e:

        conn.rollback()
        print("❌ Error in mark_unsold:", e)

        raise HTTPException(status_code=500, detail=str(e))

    finally:
        cursor.close()
        conn.close()


@router.post("/undo-sale")
async def undo_sale(request: Request):
    """
    Reverses the sale of a player (or the last sold player if player_id is not specified):
    1. Refunds the winning team's purse (purse + sold_price) & updates players_bought count.
    2. Deletes the sold_players record.
    3. Cleans current_auction and live_bids rows for that player.
    4. Notifies connected clients via Socket.IO events.
    """
    token = get_token_from_request(request)

    if not token:
        raise HTTPException(status_code=401, detail="Unauthorized")

    payload = verify_token(token)

    if not payload or payload.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")

    try:
        body = await request.json()
    except Exception:
        body = {}

    player_id = body.get("player_id")

    conn = get_db_connection()

    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor(pymysql.cursors.DictCursor)

    try:
        # If player_id provided, fetch that specific sold player; else fetch latest sold player
        if player_id:
            cursor.execute("""
                SELECT player_id, team_id, sold_price
                FROM sold_players
                WHERE player_id = %s
                LIMIT 1
            """, (player_id,))
        else:
            cursor.execute("""
                SELECT player_id, team_id, sold_price
                FROM sold_players
                ORDER BY sold_time DESC
                LIMIT 1
            """)

        sold_record = cursor.fetchone()

        if not sold_record:
            raise HTTPException(status_code=404, detail="No sold player record found to undo")

        target_player_id = sold_record["player_id"]
        winning_team_id = sold_record["team_id"]
        sold_price = float(sold_record["sold_price"] or 0)

        # 1. Refund winning team's purse & adjust players_bought count
        cursor.execute("""
            UPDATE teams 
            SET purse = purse + %s,
                players_bought = GREATEST(0, players_bought - 1)
            WHERE team_id = %s
        """, (sold_price, winning_team_id))

        cursor.execute("SELECT purse FROM teams WHERE team_id = %s", (winning_team_id,))
        team_row = cursor.fetchone()
        updated_purse = float(team_row["purse"]) if team_row else 0.0

        # 2. Delete sold_players record
        cursor.execute("DELETE FROM sold_players WHERE player_id = %s", (target_player_id,))

        # 3. Clean current_auction and live_bids
        cursor.execute("DELETE FROM current_auction WHERE player_id = %s", (target_player_id,))
        cursor.execute("DELETE FROM live_bids WHERE player_id = %s", (target_player_id,))

        conn.commit()

        # 4. Fetch player info for broadcast
        cursor.execute("""
            SELECT id, name, category, type, image_path, base_price
            FROM players
            WHERE id = %s
        """, (target_player_id,))

        player_info = cursor.fetchone()

        if player_info:
            for k, v in player_info.items():
                if isinstance(v, Decimal):
                    player_info[k] = float(v)

        if not player_info:
            player_info = {"id": target_player_id}

        # 5. Broadcast Socket Events
        winner_sid = team_sockets.get(winning_team_id)
        if winner_sid:
            await sio.emit("purse_update", {"purse": updated_purse}, to=winner_sid)

        await sio.emit("undo_sale", {
            "player_id": target_player_id,
            "player": player_info,
            "team_id": winning_team_id,
            "refunded_amount": sold_price,
            "message": f"Sale of {player_info.get('name', 'player')} reversed."
        })

        print(f"↩ Undo sale completed for player {target_player_id}. Refunded ₹{sold_price} to Team {winning_team_id}.")

        return {
            "success": True,
            "message": f"Sale of {player_info.get('name', 'player')} undone successfully",
            "player_id": target_player_id,
            "winning_team_id": winning_team_id,
            "refunded_amount": sold_price
        }

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        print("❌ Error in undo_sale:", e)
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        cursor.close()
        conn.close()

@router.post("/restart-player")
async def restart_player(request: Request):
    """
    Restarts an auction for a specific player (whether unsold, sold, or unbidded):                                 
        1. Validates Admin token.                                                                                      
        2. Cancels any active background timer task.                                                                   
        3. Removes player from unsold_players (or sold_players with purse refund).                                     
        4. Clears current_auction and live_bids tables.                                                                
        5. Inserts new record in current_auction for specified player_id.                                              
        6. Registers in-memory auction_expiry and spawns background_timer.                                             
        7. Broadcasts 'auction_started' event to connected clients.  
    """
    token = get_token_from_request(request)
    if not token:
        raise HTTPException(status_code=401, detail="Unauthorized")

    payload = verify_token(token)
    if not payload or payload.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")

    try:
        data = await request.json()
    except Exception:
        data = {}

    player_id = data.get("player_id")
    duration = data.get("duration", 120)

    if not player_id:
        raise HTTPException(status_code=400, detail="player_id is required")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor(pymysql.cursors.DictCursor)

    try:
        #1. Stop any current active auction timer task
        cursor.execute("SELECT player_id FROM current_auction LIMIT 1")
        active= cursor.fetchone()
        if active:
            stop_timer_task(active["player_id"])

        #2. clear current auction & live bids table
        cursor.execute("DELETE FROM current_auction")
        cursor.execute("DELEtE FROM live_bids")

        #3. Clean player from unsold_players if present
        cursor.execute("DELETE FROM unsold_players WHERE player_id = %s", (player_id,))

        #4. Clean player from sold_players if present (and refund winning team purse)
        cursor.execute("SELECT team_id, sold_price FROM sold_players WHERE player_id = %s", (player_id,))
        sold_rec = cursor.fetchone()
        if sold_rec:
            winning_team_id = sold_rec["team_id"]
            sold_price = float(sold_rec["sold_price"] or 0)
            cursor.execute("""
                UPDATE teams
                SET purse = purse + %s,
                    players_bought = GREATEST(0, players_bought - 1)
                WHERE team_id = %s
            """, (sold_price, winning_team_id))
            cursor.execute("DELETE FROM sold_players WHERE player_id = %s", (player_id,))

        #5. Check if player exists in players table
        cursor.execute("SELECT * FROM players WHERE id = %s", (player_id,))
        player = cursor.fetchone()
        if not player:
            raise HTTPException(status_code= 404, detail=f"Player with ID {player_id} not found")

        #Convert Decimal values for JSON safety
        for k,v in player.items():
            if isinstance(v, Decimal):
                player[k] = float(v)

        #6. Insert new current_auction row
        start_time = datetime.now(timezone.utc)
        expires_at = start_time + timedelta(seconds=duration)

        cursor.execute("""
            INSERT INTO current_auction
            (player_id, start_time, expires_at, auction_duration, mode)
            VALUES(%s,%s,%s,%s,%s)
        """,(player_id, start_time, expires_at, duration, "specific"))

        conn.commit()

        #7. Update in-memory timer expiry tracking
        auction_expiry[player_id] = expires_at

        #8. Broadcast 'auction_started' event to connected Socket clients
        await sio.emit("auction_started",{
            "player_id": player_id,
            "player_name": player["name"],
            "player": player,
            "mode": "specific",
            "duration": duration,
            "expires_at": expires_at.isoformat(),
            "current_bid": float(player.get("base_price") or 0),
            "history": []
        })

        # 9. Spawn background timer task                                                                           
        asyncio.create_task(                                                                                       
            background_timer(                                                                                      
                player_id,                                                                                         
                "specific",                                                                                        
                payload.get("session_id")                                                                          
            )                                                                                                      
        )                                                                                                          
                                                                                                                       
        print(f"🚀 Re-started auction for player {player['name']} (ID: {player_id})")                              
                                                                                                                       
        return {                                                                                                   
            "status": "auction_restarted",                                                                         
            "player_id": player_id,                                                                                
            "player_name": player["name"],                                                                         
            "duration": duration,                                                                                  
            "expires_at": expires_at.isoformat()                                                                   
        }                                                                                                          
                                                                                                                       
    except HTTPException:                                                                                          
        conn.rollback()                                                                                            
        raise                                                                                                      
    except Exception as e:                                                                                         
        conn.rollback()                                                                                            
        print("❌ Error in restart_player:", e)                                                                    
        raise HTTPException(status_code=500, detail=str(e))                                                        
    finally:                                                                                                       
        cursor.close()                                                                                             
        conn.close() 