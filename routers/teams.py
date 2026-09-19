from fastapi import APIRouter, UploadFile, File, HTTPException, Request, Form
from auth.auth_handler import verify_token, get_token_from_request
from core.database import get_db_connection
import pymysql
import os
import io
import uuid
from typing import Optional

router = APIRouter()

UPLOAD_FOLDER_TEAMS = "uploads/teams"
ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg", "webp", "jfif"}

#---------- GET ALL TEAMS ------------
@router.get("/teams")
def get_teams():
    conn = get_db_connection()
    if conn is None:
        return {"error": "Database connection failed"}
    
    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        cursor.execute("""
            SELECT
                team_id,
                name,
                captain,
                team_rank AS trank,
                total_budget AS total_budget,
                season_budget AS current_budget,
                players_bought AS players_bought,
                image_path
            FROM teams
            ORDER BY name ASC
        """)

        teams = cursor.fetchall()
        for t in teams:
            if not t.get("image_path"):
                t["image_path"] = None

        return {
            "success": True,
            "count": len(teams),
            "teams": teams
        }
    
    except Exception as e:
        print("Team route error:", e)
        return {"error": str(e)}

    finally:
        cursor.close()
        conn.close()

#---------- GET TEAM SQUAD -----------
@router.get("/team/{team_id}")
def get_team_by_id(team_id: int):
    conn = get_db_connection()

    if conn is None:
        return {"error": "Database connection failed"}

    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        cursor.execute("""
            SELECT
                p.id AS player_id,
                p.name,
                p.category,
                p.type,
                p.image_path,
                sp.sold_price,
                sp.sold_time
            FROM sold_players sp
            JOIN players p ON sp.player_id = p.id
            WHERE sp.team_id = %s
            ORDER BY sp.sold_time ASC       
        """, (team_id,))

        squad = cursor.fetchall()

        # Fetch team details
        cursor.execute("SELECT * FROM teams WHERE team_id = %s", (team_id,))
        team_row = cursor.fetchone()

        return {
            "success": True,
            "team_id": team_id,
            "team": team_row,
            "players": squad
        }
    
    except Exception as e:
        print("Team route error:", e)
        return {"error": str(e)}
    
    finally:
        cursor.close()
        conn.close()


#---------- ADD TEAM ------------
@router.post("/add-team")
async def add_team(
    request: Request,

    # -------- FORM FIELDS --------
    teamName: Optional[str] = Form(None),
    captain: Optional[str] = Form(None),
    teamRank: Optional[int] = Form(None),
    totalBudget: Optional[float] = Form(None),
    seasonBudget: Optional[float] = Form(None),
    playersBought: Optional[int] = Form(None),
    mobile: Optional[str] = Form(None),
    emailId: Optional[str] = Form(None),
    password: Optional[str] = Form(None),

    # -------- FILE --------
    image: Optional[UploadFile] = File(None)
):
    # ================= AUTH =================
    token = get_token_from_request(request)

    if not token:
        raise HTTPException(status_code=401, detail="Unauthorized")

    user = verify_token(token)

    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")

    # ================= VALIDATION =================
    if not teamName:
        raise HTTPException(status_code=400, detail="Team name is required")

    # ================= IMAGE UPLOAD =================
    image_path = None

    if image:
        img_content = await image.read()
        from core.image_handler import validate_image_bytes
        validate_image_bytes(img_content)
        
        try:
            from PIL import Image
            from core.image_handler import crop_and_resize_to_square, compress_to_webp, upload_image_to_supabase
            
            pil_image = Image.open(io.BytesIO(img_content))
            processed_image = crop_and_resize_to_square(pil_image, 400)
            webp_bytes = compress_to_webp(processed_image)
            
            filename = f"{uuid.uuid4().hex}.webp"
            storage_path = f"teams/{filename}"
            image_path = upload_image_to_supabase(webp_bytes, storage_path)
        except HTTPException:
            raise
        except Exception as e:
            print("❌ Error processing/uploading team logo in add_team:", e)
            raise HTTPException(status_code=500, detail=f"Image upload failed: {str(e)}")

    # ================= NORMALIZE VALUES =================
    teamRank = teamRank or 0
    totalBudget = totalBudget or 0
    seasonBudget = seasonBudget or 0
    playersBought = playersBought or 0

    # ================= DB =================
    conn = get_db_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor(pymysql.cursors.DictCursor)

    try:
        cursor.execute("""
            INSERT INTO teams 
            (name, captain, mobile_no, email_id, team_rank, total_budget, season_budget, purse, players_bought, image_path)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING team_id
        """, (
            teamName,
            captain,
            mobile,
            emailId,
            teamRank,
            totalBudget,
            seasonBudget,
            seasonBudget,
            playersBought,
            image_path
        ))
        new_team_row = cursor.fetchone()
        new_team_id = new_team_row["team_id"] if new_team_row else None
        conn.commit()

        # Automated Supabase auth user creation
        if emailId and new_team_id:
            try:
                from core.supabase_client import get_supabase_admin_client
                supabase_admin = get_supabase_admin_client()

                team_password = password if password else "JPLTeam@2026"
                supabase_admin.auth.admin.create_user({
                    "email": emailId,
                    "password": team_password,
                    "email_confirm": True,
                    "user_metadata": {
                        "name": teamName,
                        "role": "team",
                        "team_id": new_team_id
                    },
                    "app_metadata": {
                        "role": "team"
                    }
                })
                print(f"✅ Created Supabase Auth User for '{teamName}' (Email: {emailId}, Team ID: {new_team_id})")
            except Exception as auth_err:
                print("⚠ Warning: Team created in DB, but failed to create Supabase Auth User:", auth_err)

        return {
            "message": "Team added successfully!",
            "team_id": new_team_id
        }

    except pymysql.IntegrityError:
        conn.rollback()
        raise HTTPException(
            status_code=400,
            detail="Team name already exists"
        )

    except Exception as e:
        conn.rollback()
        print("❌ add-team error:", e)
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        cursor.close()
        conn.close()


#---------- UPDATE TEAM ------------
@router.put("/team/{team_id}")
async def update_team(
    team_id: int,
    request: Request,

    # -------- FORM / JSON FIELDS --------
    teamName: Optional[str] = Form(None),
    captain: Optional[str] = Form(None),
    teamRank: Optional[int] = Form(None),
    totalBudget: Optional[float] = Form(None),
    seasonBudget: Optional[float] = Form(None),
    purse: Optional[float] = Form(None),
    playersBought: Optional[int] = Form(None),
    mobile: Optional[str] = Form(None),
    emailId: Optional[str] = Form(None),
    password: Optional[str] = Form(None),

    # -------- FILE --------
    image: Optional[UploadFile] = File(None)
):
    # ================= AUTH =================
    token = get_token_from_request(request)

    if not token:
        raise HTTPException(status_code=401, detail="Unauthorized")

    user = verify_token(token)

    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")

    # If application/json was sent instead of form-data, extract fields from JSON body
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        try:
            json_body = await request.json()
            teamName = json_body.get("teamName") or json_body.get("name") or teamName
            captain = json_body.get("captain", captain)
            teamRank = json_body.get("teamRank") or json_body.get("team_rank") or teamRank
            totalBudget = json_body.get("totalBudget") or json_body.get("total_budget") or totalBudget
            seasonBudget = json_body.get("seasonBudget") or json_body.get("season_budget") or seasonBudget
            purse = json_body.get("purse", purse)
            playersBought = json_body.get("playersBought") or json_body.get("players_bought") or playersBought
            mobile = json_body.get("mobile") or json_body.get("mobile_no") or mobile
            emailId = json_body.get("emailId") or json_body.get("email_id") or emailId
            password = json_body.get("password", password)
        except Exception:
            pass

    conn = get_db_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor(pymysql.cursors.DictCursor)

    try:
        # Check if team exists
        cursor.execute("SELECT * FROM teams WHERE team_id = %s", (team_id,))
        existing_team = cursor.fetchone()
        if not existing_team:
            raise HTTPException(status_code=404, detail=f"Team with ID {team_id} not found")

        # ================= IMAGE UPLOAD =================
        image_path = None
        if image:
            img_content = await image.read()
            from core.image_handler import validate_image_bytes
            validate_image_bytes(img_content)

            try:
                from PIL import Image
                from core.image_handler import crop_and_resize_to_square, compress_to_webp, upload_image_to_supabase

                pil_image = Image.open(io.BytesIO(img_content))
                processed_image = crop_and_resize_to_square(pil_image, 400)
                webp_bytes = compress_to_webp(processed_image)

                filename = f"{uuid.uuid4().hex}.webp"
                storage_path = f"teams/{filename}"
                image_path = upload_image_to_supabase(webp_bytes, storage_path)
            except HTTPException:
                raise
            except Exception as e:
                print("❌ Error processing/uploading team logo in update_team:", e)
                raise HTTPException(status_code=500, detail=f"Image upload failed: {str(e)}")

        # Build dynamic update statement
        update_fields = []
        params = []

        if teamName is not None:
            update_fields.append("name = %s")
            params.append(teamName)
        if captain is not None:
            update_fields.append("captain = %s")
            params.append(captain)
        if mobile is not None:
            update_fields.append("mobile_no = %s")
            params.append(mobile)
        if emailId is not None:
            update_fields.append("email_id = %s")
            params.append(emailId)
        if teamRank is not None:
            update_fields.append("team_rank = %s")
            params.append(teamRank)
        if totalBudget is not None:
            update_fields.append("total_budget = %s")
            params.append(totalBudget)
        if seasonBudget is not None:
            update_fields.append("season_budget = %s")
            params.append(seasonBudget)
        if purse is not None:
            update_fields.append("purse = %s")
            params.append(purse)
        if playersBought is not None:
            update_fields.append("players_bought = %s")
            params.append(playersBought)
        if image_path is not None:
            update_fields.append("image_path = %s")
            params.append(image_path)

        if not update_fields:
            return {"success": True, "message": "No fields provided to update", "team_id": team_id}

        params.append(team_id)
        set_clause = ", ".join(update_fields)
        cursor.execute(f"UPDATE teams SET {set_clause} WHERE team_id = %s", tuple(params))
        conn.commit()

        # Update Supabase Auth user password if provided
        if password and (emailId or existing_team.get("email_id")):
            try:
                from core.supabase_client import get_supabase_admin_client
                supabase_admin = get_supabase_admin_client()
                target_email = emailId or existing_team.get("email_id")
                users_res = supabase_admin.auth.admin.list_users()
                for u in users_res:
                    if u.email == target_email or u.user_metadata.get("team_id") == team_id:
                        supabase_admin.auth.admin.update_user_by_id(u.id, {"password": password})
                        print(f"✅ Updated password for Supabase Auth user {u.email}")
                        break
            except Exception as auth_err:
                print("⚠ Warning: Failed to update Supabase Auth User password:", auth_err)

        return {
            "success": True,
            "message": "Team updated successfully!",
            "team_id": team_id
        }

    except pymysql.IntegrityError:
        conn.rollback()
        raise HTTPException(
            status_code=400,
            detail="Team name already exists"
        )
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        print("❌ update-team error:", e)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


#---------- DELETE TEAM ------------
@router.delete("/team/{team_id}")
async def delete_team(team_id: int, request: Request):
    # ================= AUTH =================
    token = get_token_from_request(request)

    if not token:
        raise HTTPException(status_code=401, detail="Unauthorized")

    user = verify_token(token)

    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")

    conn = get_db_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor(pymysql.cursors.DictCursor)

    try:
        # Check if team exists
        cursor.execute("SELECT * FROM teams WHERE team_id = %s", (team_id,))
        team = cursor.fetchone()
        if not team:
            raise HTTPException(status_code=404, detail=f"Team with ID {team_id} not found")

        # Delete team image from bucket
        image_path = team.get("image_path")
        if image_path:
            try:
                from core.image_handler import delete_image_from_supabase
                delete_image_from_supabase(image_path)
            except Exception as img_err:
                print(f"⚠️ Warning deleting team image: {img_err}")

        # Clean related records across tables
        cursor.execute("DELETE FROM live_bids WHERE team_id = %s", (team_id,))
        cursor.execute("DELETE FROM bids WHERE team_id = %s", (team_id,))
        cursor.execute("DELETE FROM sold_players WHERE team_id = %s", (team_id,))
        cursor.execute("DELETE FROM player_teams WHERE team_id = %s", (team_id,))
        cursor.execute("UPDATE captains SET team_id = NULL WHERE team_id = %s", (team_id,))
        cursor.execute("UPDATE users SET team_id = NULL WHERE team_id = %s", (team_id,))

        # Delete the team
        cursor.execute("DELETE FROM teams WHERE team_id = %s", (team_id,))
        conn.commit()

        # Delete Supabase Auth User if exists
        try:
            from core.supabase_client import get_supabase_admin_client
            supabase_admin = get_supabase_admin_client()
            users_res = supabase_admin.auth.admin.list_users()
            for u in users_res:
                if u.user_metadata.get("team_id") == team_id or (team.get("email_id") and u.email == team.get("email_id")):
                    supabase_admin.auth.admin.delete_user(u.id)
                    print(f"✅ Deleted Supabase Auth User {u.email} for Team {team_id}")
                    break
        except Exception as auth_err:
            print("⚠ Warning: Failed to delete Supabase Auth User:", auth_err)

        return {
            "success": True,
            "message": f"Team '{team.get('name')}' (ID: {team_id}) deleted successfully!"
        }

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        print("❌ delete-team error:", e)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()
