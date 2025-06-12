import json

def count_json_data(json_file_path):
    """
    Conta elementos no arquivo JSON: imagens, tênis e dados demográficos
    """
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # Count unique images
    list_of_images = []
    for i in data.get('filename', {}):
        image = data['filename'][i]
        list_of_images.append(image)
    image_count = len(set(list_of_images))
    # Count total shoes
    shoe_count = 0
    for detections in data.get('shoes', {}).values():
        shoe_count += len(detections)
    
    # Count demographics length of object
    demographic_count = len(data.get('demographic', {}))
    return image_count, shoe_count, demographic_count

from argparse import ArgumentParser

if __name__ == "__main__":
    parser = ArgumentParser(description="Conta elementos em arquivo JSON de detecções")
    parser.add_argument("json_file", help="Caminho para o arquivo JSON")
    args = parser.parse_args()
    
    images, shoes, demographics = count_json_data(args.json_file)
    
    print(f"Images: {images}")
    print(f"Shoes: {shoes}")
    print(f"Demographics: {demographics}")