import io
import os
from PIL import Image
from fastapi import HTTPException
from core.supabase_client import get_supabase_admin_client

BUCKET_NAME = os.getenv("SUPABASE_STORAGE_BUCKET", "auctra-uploads")
MAX_FILE_SIZE = 5 * 1024 * 1024  # 5 MB limit

def validate_image_bytes(image_bytes: bytes, max_size: int = MAX_FILE_SIZE):
    """Validates that the file size is within limits and that Pillow can open the image."""
    if len(image_bytes) > max_size:
        raise HTTPException(status_code=400, detail="File size exceeds the 5MB limit")
    try:
        img = Image.open(io.BytesIO(image_bytes))
        img.load()
    except Exception as e:
        print("❌ Image validation error:", e)
        raise HTTPException(status_code=400, detail="Invalid or corrupted image file format")

def crop_and_resize_to_square(image: Image.Image, target_size: int) -> Image.Image:
    """Center crops an image to 1:1 aspect ratio and resizes it to target_size."""
    width, height = image.size
    min_dim = min(width, height)
    
    # Calculate crop coordinates
    left = (width - min_dim) / 2
    top = (height - min_dim) / 2
    right = (width + min_dim) / 2
    bottom = (height + min_dim) / 2
    
    cropped = image.crop((left, top, right, bottom))
    
    # Use LANCZOS for high quality resizing (standard in modern Pillow)
    try:
        resampling = Image.Resampling.LANCZOS
    except AttributeError:
        # Fallback for older Pillow versions
        resampling = Image.ANTIALIAS
        
    return cropped.resize((target_size, target_size), resampling)

def compress_to_webp(image: Image.Image, quality: int = 80) -> bytes:
    """Saves the image into a WebP byte buffer with specified quality."""
    buffer = io.BytesIO()
    # Convert RGBA to RGB if saving as WebP to avoid profile mismatch issues
    if image.mode in ("RGBA", "LA"):
        background = Image.new("RGBA", image.size, (255, 255, 255))
        alpha_composite = Image.alpha_composite(background, image)
        image = alpha_composite.convert("RGB")
    elif image.mode != "RGB":
        image = image.convert("RGB")
        
    image.save(buffer, format="WEBP", quality=quality)
    return buffer.getvalue()

def upload_image_to_supabase(image_bytes: bytes, path: str) -> str:
    """
    Uploads bytes to the Supabase Storage bucket and returns the public CDN URL.
    The path should be like 'players/filename.webp' or 'teams/filename.webp'.
    """
    supabase = get_supabase_admin_client()
    
    # Upload to Supabase Storage
    # We use upsert=True to allow overwriting if the same file is uploaded again
    supabase.storage.from_(BUCKET_NAME).upload(
        path=path,
        file=image_bytes,
        file_options={"content-type": "image/webp", "x-upsert": "true"}
    )
    
    # Get the public URL of the uploaded image
    res = supabase.storage.from_(BUCKET_NAME).get_public_url(path)
    return res

def delete_image_from_supabase(path: str):
    """Deletes an image file from the Supabase Storage bucket."""
    try:
        supabase = get_supabase_admin_client()
        
        # If the path is a full URL, extract the actual storage path
        # Example URL: https://<project>.supabase.co/storage/v1/object/public/<bucket>/players/<uuid>.webp
        if path and path.startswith("http"):
            # Extract everything after the bucket name
            marker = f"/{BUCKET_NAME}/"
            if marker in path:
                path = path.split(marker)[-1]
                
        supabase.storage.from_(BUCKET_NAME).remove([path])
    except Exception as e:
        print(f"⚠️ Failed to delete image from Supabase storage: {e}")
