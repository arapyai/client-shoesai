import math
from io import BytesIO

import streamlit as st
from PIL import Image, ImageDraw
import requests

from database_abstraction import get_marathon_list_from_db, get_images_paginated
from ui_components import page_header_with_logout, check_auth, create_column_grid

IMAGE_SERVER = "http://localhost:8000/"  # URL do servidor de imagens

st.set_page_config(layout="wide", page_title="Shoes AI - Imagens")

user_id = check_auth(admin_only=True)

# Header
page_header_with_logout("🖼️ Navegar Imagens", "Selecione uma prova para visualizar as detecções", key_suffix="gallery")

# Load marathon options
@st.cache_data
def fetch_marathon_options():
    return get_marathon_list_from_db()

if "marathon_options" not in st.session_state:
    st.session_state.marathon_options = fetch_marathon_options()

MARATHON_ID_MAP = {m["name"]: m["id"] for m in st.session_state.marathon_options}
MARATHON_NAMES = list(MARATHON_ID_MAP.keys())

selected_name = st.selectbox("Prova", MARATHON_NAMES)
selected_id = MARATHON_ID_MAP.get(selected_name)

IMAGES_PER_PAGE = 8
if "image_page" not in st.session_state:
    st.session_state.image_page = 0

if selected_id:
    data = get_images_paginated(selected_id, offset=st.session_state.image_page * IMAGES_PER_PAGE, limit=IMAGES_PER_PAGE)
    total_pages = math.ceil(data["total"] / IMAGES_PER_PAGE) if data["total"] else 1

    nav_cols = st.columns(3)
    with nav_cols[0]:
        if st.button("⬅️ Anterior", disabled=st.session_state.image_page == 0):
            st.session_state.image_page -= 1
            st.rerun()
    with nav_cols[1]:
        st.write(f"Página {st.session_state.image_page + 1} de {total_pages}")
    with nav_cols[2]:
        if st.button("Próxima ➡️", disabled=st.session_state.image_page >= total_pages - 1):
            st.session_state.image_page += 1
            st.rerun()

    cols = create_column_grid(len(data["images"]), 4)
    for col, img in zip(cols, data["images"]):
        url = f"{IMAGE_SERVER.rstrip('/')}/{img['filename']}"
        print(f"Image URL: {url}")  # Debugging line
        col.image(url, use_container_width=True)
        if col.button("Ver", key=f"view_{img['image_id']}"):
            st.session_state.selected_image = img
            st.session_state.show_modal = True
            st.rerun()

if st.session_state.get("show_modal") and st.session_state.get("selected_image"):
    img_data = st.session_state.selected_image
    image_url = f"{IMAGE_SERVER.rstrip('/')}/{img_data['filename']}"
    try:
        resp = requests.get(image_url)
        if resp.status_code == 200:
            pil_img = Image.open(BytesIO(resp.content)).convert("RGB")
            draw = ImageDraw.Draw(pil_img)
            for shoe in img_data.get("shoes", []):
                bbox = shoe.get("bbox")
                if bbox and all(v is not None for v in bbox):
                    draw.rectangle(bbox, outline="green", width=3)
            demo = img_data.get("demographic")
            if demo:
                bbox = demo.get("bbox")
                if bbox and all(v is not None for v in bbox):
                    draw.rectangle(bbox, outline="red", width=3)
            st.image(pil_img, use_container_width=True)
        else:
            st.error("Falha ao carregar a imagem")
    except Exception as e:
        st.error(f"Erro ao carregar imagem: {e}")

    st.write(f"**Arquivo:** {img_data['filename']}")
    if img_data.get("shoes"):
        st.write("**Tênis:**")
        for shoe in img_data["shoes"]:
            st.write(f"- {shoe['brand']} (prob={shoe['probability']:.2f})")
    if img_data.get("demographic"):
        demo = img_data["demographic"]
        st.write(f"**Gênero:** {demo['gender']['label']} ({demo['gender']['prob']:.2f})")
        st.write(f"**Idade:** {demo['age']['label']} ({demo['age']['prob']:.2f})")
        st.write(f"**Raça:** {demo['race']['label']} ({demo['race']['prob']:.2f})")
    if st.button("Fechar", key="close_modal"):
        st.session_state.show_modal = False
        st.rerun()
