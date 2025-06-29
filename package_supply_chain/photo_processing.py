from PIL import Image as PILImage
import io

def resize_image(image_bytes, max_size=(700, 700)):
    """Redimensionne une image tout en conservant son ratio d'aspect.
    
    Args:
        image_bytes (bytes): L'image en format bytes
        max_size (tuple): La taille maximale (largeur, hauteur)
    
    Returns:
        bytes: L'image redimensionnée en format bytes
    """
    # Ouvre l'image depuis les bytes
    img = PILImage.open(io.BytesIO(image_bytes))
    
    # Convertit en RGB si nécessaire (pour les images PNG avec transparence)
    if img.mode in ('RGBA', 'P'):
        img = img.convert('RGB')
    
    # Calcule les nouvelles dimensions en conservant le ratio
    ratio = min(max_size[0] / img.width, max_size[1] / img.height)
    if ratio < 1:  # Seulement si l'image est plus grande que max_size
        new_size = (int(img.width * ratio), int(img.height * ratio))
        img = img.resize(new_size, PILImage.Resampling.LANCZOS)
    
    # Convertit l'image redimensionnée en bytes
    output = io.BytesIO()
    img.save(output, format='JPEG', quality=85, optimize=True)
    return output.getvalue()
