import asyncio
from datetime import datetime, timezone, timedelta
import pymysql

from core.database import get_db_connection
from sockets.socket_manager import sio, team_sockets

auction_expiry = {}
active_timer_tasks = {}

def stop_timer_task(player_id):
    """Cancels and cleans up active background_timer asyncio Task for player_id."""
    task = active_timer_tasks.pop(player_id, None)
    auction_expiry.pop(player_id, None)
    if task and not task.done():
        task.cancel()
        print(f"🛑 Cancelled background timer task for player {player_id}")

async def background_timer(player_id, mode, session_id):

    # Track active task for cancellation on pause/cancel
    active_timer_tasks[player_id] = asyncio.current_task()

    print(f"⏰ Timer started for player {player_id}")

    # Fetch initial auction state ONCE when timer starts
    conn = get_db_connection()
    if not conn:
        print("⚠ DB connection failed in background_timer")
        return

    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(
            "SELECT paused, paused_remaining, expires_at FROM current_auction WHERE player_id=%s",
            (player_id,)
        )
        state = cursor.fetchone()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    if not state or not state.get("expires_at"):
        print("⚠ Auction row missing — stopping timer")
        return

    db_expires = state["expires_at"]
    if isinstance(db_expires, str):
        db_expires = datetime.fromisoformat(db_expires)

    if db_expires.tzinfo is None:
        db_expires = db_expires.replace(tzinfo=timezone.utc)

    # Store initial expiry time in shared memory
    auction_expiry[player_id] = db_expires 

    try:
        while True:
            loop_start = datetime.now(timezone.utc)
            now = datetime.now(timezone.utc)

            # Dynamically read expiry from shared memory (reflects extension instantly)                                              
            current_expires = auction_expiry.get(player_id, db_expires)
            remaining = max(0, int((current_expires - now).total_seconds()))                                                         

            if remaining <= 0:
                break

            await sio.emit("timer_update", {
                "player_id": player_id,
                "remaining_seconds": remaining,
                "server_time": now.isoformat()
            })

            # Calculate exact elapsed processing time to compensate for drift (1.0s target loop time)
            elapsed = (datetime.now(timezone.utc) - loop_start).total_seconds()
            sleep_duration = max(0.1, 1.0 - elapsed)
            await asyncio.sleep(sleep_duration)
    except asyncio.CancelledError:
        print(f"🛑 Background timer task cleanly stopped for player {player_id}")
        auction_expiry.pop(player_id, None)
        active_timer_tasks.pop(player_id, None)
        return
        
    print("⏰ Timer expired")
    auction_expiry.pop(player_id, None)

    conn = get_db_connection()
    if not conn:
        print("⚠ DB connection failed after timer expiration")
        return
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    try:

        cursor.execute(
            "SELECT * FROM current_auction WHERE player_id=%s FOR UPDATE",
            (player_id,)
        )

        auction = cursor.fetchone()

        if not auction:
            return

        # ---------------- HIGHEST BID ----------------
        cursor.execute("""
        SELECT b.team_id, b.bid_amount, t.name AS team_name,t.image_path
        FROM live_bids b
        JOIN teams t ON b.team_id = t.team_id
        WHERE b.player_id = %s
        ORDER BY b.bid_amount DESC
        LIMIT 1
        """, (player_id,))

        top_bid = cursor.fetchone()

        if top_bid:

            cursor.execute("""
            UPDATE teams
            SET purse = GREATEST(0.0, purse - %s),
                Players_Bought = COALESCE(Players_Bought, 0) + 1
            WHERE team_id = %s
            """, (top_bid["bid_amount"], top_bid["team_id"]))

            cursor.execute(
                "SELECT purse FROM teams WHERE team_id=%s",
                (top_bid["team_id"],)
            )
            row = cursor.fetchone()
            updated_purse = float(row["purse"]) if row and row.get("purse") else 0.0
            winner_sid = team_sockets.get(top_bid["team_id"])

            if winner_sid:
                await sio.emit(
                    "purse_update",
                    {"purse": updated_purse},
                    to= winner_sid
                )

            cursor.execute("""
            INSERT INTO sold_players
            (player_id, team_id, sold_price, sold_time)
            VALUES (%s,%s,%s,NOW())
            """, (
                player_id,
                top_bid["team_id"],
                top_bid["bid_amount"]
            ))

            # Fetch player info
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
                          
            await sio.emit("auction_ended", {
                "status": "sold",
                "player": player_info,
                "team": {
                    "team_id": top_bid["team_id"],
                    "team_name": top_bid["team_name"],
                    "bid_amount": float(top_bid["bid_amount"]),
                    "image_path": top_bid.get("image_path")
                },
                "message": f"Player sold to {top_bid['team_name']} for ₹{top_bid['bid_amount']}"
         })

            await sio.emit("next_player_loading", {
                "delay": 10
            })
        else:

            cursor.execute("""
            INSERT INTO unsold_players
            (player_id, reason, added_on)
            VALUES (%s,%s,NOW())
            """, (player_id, "No Bids"))

            # Fetch player info
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
                        
            await sio.emit("auction_ended", {
                "status": "unsold",
                "player": player_info,
                "message": "No bids received — player marked UNSOLD"
            })

            await sio.emit("next_player_loading", {
                "delay": 10
            })

        cursor.execute("DELETE FROM current_auction WHERE player_id=%s", (player_id,))
        cursor.execute("DELETE FROM live_bids WHERE player_id=%s", (player_id,))

        if conn:
            conn.commit()

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    # ---------------- DELAY BEFORE NEXT PLAYER ----------------
    print("⏳ Waiting 10 seconds before next player")
    await asyncio.sleep(10)

    conn = get_db_connection()
    if not conn:
        print("⚠ DB connection failed before next player selection")
        return
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    try:

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
            print("🏁 Auction finished")
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
            mode
        ))

        if conn:
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

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        active_timer_tasks.pop(player_id, None)
        auction_expiry.pop(player_id, None)                                                                                     


# async def load_next_player_after_delay():

#     print("⏳ Waiting 10 seconds before next player")

#     await asyncio.sleep(10)

#     conn = get_db_connection()
#     cursor = conn.cursor(pymysql.cursors.DictCursor)

#     try:

#         cursor.execute("""
#         SELECT *
#         FROM players
#         WHERE id NOT IN (
#             SELECT player_id FROM sold_players
#             UNION
#             SELECT player_id FROM unsold_players
#         )
#         ORDER BY RAND()
#         LIMIT 1
#         """)

#         player = cursor.fetchone()

#         if not player:
#             await sio.emit("auction_finished")
#             return

#         duration = 120
#         start_time = datetime.now(timezone.utc)
#         expires_at = start_time + timedelta(seconds=duration)

#         cursor.execute("""
#         INSERT INTO current_auction
#         (player_id,start_time,expires_at,auction_duration,mode)
#         VALUES (%s,%s,%s,%s,%s)
#         """, (
#             player["id"],
#             start_time,
#             expires_at,
#             duration,
#             "random"
#         ))

#         conn.commit()

#         await sio.emit("auction_started", {
#             "player": {
#                 "id": player["id"],
#                 "name": player["name"],
#                 "image_path": player["image_path"],
#                 "jersey": player["jersey"],
#                 "category": player["category"],
#                 "type": player["type"],
#                 "base_price": float(player["base_price"]),
#                 "highest_runs": player["highest_runs"]
#             },
#             "duration": duration,
#             "expires_at": expires_at.isoformat(),
#             "current_bid": float(player["base_price"]),
#             "history": []
#         })

#     finally:
#         cursor.close()
#         conn.close()