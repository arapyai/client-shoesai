import streamlit as st
import pandas as pd
import altair as alt
import math
from typing import Optional, List, Dict, Any


# --- Utility Functions ---

def create_column_grid(num_items: int, items_per_row: int) -> List[Any]:
    """
    Creates a grid of Streamlit columns and returns them as a flat list.

    Args:
        num_items: The total number of items to display
        items_per_row: The maximum number of items (columns) per visual row

    Returns:
        A flat list of Streamlit column objects
    """
    if num_items <= 0 or items_per_row <= 0:
        return []

    flat_columns_list = []
    num_rows = math.ceil(num_items / items_per_row)

    for i in range(num_rows):
        start_index = i * items_per_row
        num_cols_in_this_row = min(items_per_row, num_items - start_index)
        
        if num_cols_in_this_row > 0:
            row_cols = st.columns(num_cols_in_this_row)
            flat_columns_list.extend(row_cols)
            
    return flat_columns_list


# --- Reusable Chart Components ---

def create_bar_chart(
    data: pd.DataFrame,
    x_col: str,
    y_col: str,
    title: str = "",
    height: int = 400,
    highlight_condition: Optional[str] = None,
    highlight_color: str = "#ff6b6b",
    default_color: str = "#1f77b4"
) -> alt.Chart:
    """
    Create a standardized bar chart with optional highlighting.
    """
    color_condition = (
        alt.condition(highlight_condition, alt.value(highlight_color), alt.value(default_color))
        if highlight_condition else alt.value(default_color)
    )
    
    #Check max value for x_col to set appropriate scale
    if data[x_col].max() < 50:
        x_scale = alt.Scale(domain=[0, 50])
    elif data[x_col].max() < 75:
        x_scale = alt.Scale(domain=[0, 75])
    elif data[x_col].max() < 100:
        x_scale = alt.Scale(domain=[0, 100])
    else:
        x_scale = alt.Scale(domain=[0, data[x_col].max() * 1.1])

    chart = alt.Chart(data).mark_bar().encode(
        x=alt.X(f'{x_col}:Q', title=x_col.replace('_', ' ').title(), scale=x_scale),
        y=alt.Y(f'{y_col}:N', title=y_col.replace('_', ' ').title(), sort='-x'),
        color=color_condition,
        tooltip=[
            alt.Tooltip(f'{y_col}:N', title=y_col.replace('_', ' ').title()),
            alt.Tooltip(f'{x_col}:Q', title=x_col.replace('_', ' ').title(), format='.1f')
        ]
    ).properties(title=title, height=height)
    
    return chart


def prepare_demographic_data_for_chart(
    demographic_data: pd.DataFrame,
    min_percentage: float = 2.0
) -> pd.DataFrame:
    """
    Prepare demographic data for stacked bar charts.
    Groups small brands into 'Outros' category.
    """
    if demographic_data is None or demographic_data.empty:
        return pd.DataFrame()
    
    data = demographic_data.copy().T
    
    if data.index.name is None:
        data.index.name = "shoe_brand"
    
    brand_col_name = data.index.name
    
    # Calculate total counts and percentages
    data['total'] = data.sum(axis=1)
    total_shoes = data['total'].sum()
    data['percentage'] = (data['total'] / total_shoes) * 100
    
    # Group small brands
    small_brands = data[data['percentage'] < min_percentage].index
    if len(small_brands) > 0:
        others_row = data.loc[small_brands].sum()
        data = data.drop(small_brands)
        data.loc['Outros'] = others_row
    
    # Remove helper columns
    data = data.drop(['total', 'percentage'], axis=1)
    
    # Convert to long format
    df_long = data.reset_index().melt(
        id_vars=brand_col_name,
        var_name="demographic_category",
        value_name="count"
    )
    
    if df_long.empty or df_long['count'].sum() == 0:
        return pd.DataFrame()
    
    # Calculate percentages within each brand
    brand_totals = df_long.groupby(brand_col_name)['count'].sum().reset_index()
    brand_totals.columns = [brand_col_name, 'brand_total']
    
    df_long = pd.merge(df_long, brand_totals, on=brand_col_name)
    df_long['percentage'] = df_long['count'] / df_long['brand_total']
    
    return df_long

# --- UI Components for Main App ---

def logout_button(key_suffix: str = "", button_text: str = "🚪 Sair", help_text: str = "Fazer logout") -> bool:
    """
    Reusable logout button component that clears session state and redirects to login.
    
    Args:
        key_suffix: Suffix to add to the button key to ensure uniqueness
        button_text: Text to display on the button
        help_text: Tooltip text for the button
    
    Returns:
        True if logout button was clicked, False otherwise
    """
    if st.button(button_text, key=f"logout_button_{key_suffix}", help=help_text, use_container_width=True):
        # Clear session state
        st.session_state.logged_in = False
        st.session_state.user_info = None
        # Redirect to main login page
        st.switch_page("app.py")
        st.stop()
        return True
    return False

def add_sidebar_profile_and_logout():
    """
    Adds profile link and logout button to the bottom of the sidebar.
    This function should be called once on each page.
    """
    # Add space to push content to bottom of sidebar
    for _ in range(5):
        st.sidebar.write("")
    
    # Add a separator
    st.sidebar.markdown("---")
    
    # Display user info
    user_email = st.session_state.user_info.get("email", "Usuário")
    display_name = user_email.split("@")[0]  # Use part before @ as display name
    
    # Container for profile link
    with st.sidebar.container():
        profile_col, _ = st.columns([1, 0.2])
        with profile_col:
            if st.button(f"👤 Perfil ({display_name})", key="profile_button_sidebar", use_container_width=True):
                st.switch_page("pages/4_👤_Perfil.py")
    
    # Logout button
    with st.sidebar.container():
        logout_col, _ = st.columns([1, 0.2])
        with logout_col:
            logout_button("sidebar")

def page_header_with_logout(title, subtitle=None, key_suffix=""):
    """
    Reusable page header component that also adds profile/logout to sidebar bottom.
    
    Args:
        title (str): Main title for the page
        subtitle (str, optional): Subtitle or caption for the page
        key_suffix (str): Suffix to add to identify components (not used for logout now)
    """
    # Display the header title and subtitle
    st.title(title)
    if subtitle:
        st.caption(subtitle)
    
    # Add profile and logout to sidebar
    add_sidebar_profile_and_logout()

def display_header():
    # Create header
    st.markdown(f"## 🏃 Análise de Provas (Pastas)")
    st.caption("Aqui você pode gerar relatórios e exportá-los. Selecione as \"provas\" (pastas de imagens) que gostaria de analisar.")
    
    # Add profile and logout to sidebar
    add_sidebar_profile_and_logout()

def render_marathon_info_cards(selected_marathon_names, marathon_specific_data_for_cards, db_marathon_metadata_list):
    """
    Renders cards for each selected marathon.
    marathon_specific_data_for_cards: dict from processed_metrics with counts per marathon
    db_marathon_metadata_list: list of dicts from get_marathon_list_from_db, containing metadata like date, location
    """
    if not selected_marathon_names:
        return
    
    cols_needed = len(selected_marathon_names)
    if cols_needed == 0: return

    cols = st.columns(cols_needed)
    for i, marathon_name in enumerate(selected_marathon_names):
        card_data = marathon_specific_data_for_cards.get(marathon_name, {})
        
        # Find metadata for this marathon from the list fetched from DB
        marathon_meta = next((m for m in db_marathon_metadata_list if m['name'] == marathon_name), None)
        event_date = marathon_meta.get('event_date', "XX/XX/XXXX") if marathon_meta else "XX/XX/XXXX"
        location = marathon_meta.get('location', "Local Desconhecido") if marathon_meta else "Local Desconhecido"
        # distance = marathon_meta.get('distance_km', "Distância Desconhecida") if marathon_meta else "Distância Desconhecida"


        with cols[i]:
            with st.container(border=True):
                st.subheader("Dados Gerais")
                st.caption(f"🗓️ {event_date} | 📍 {location}")
                # st.caption(f"📏 {distance} km") # You can add distance if it's in your Marathons table and fetched
                st.caption(f"🖼️ {card_data.get('images_count', 'N/A')} Imagens")
                st.caption(f"👟 {card_data.get('shoes_count','N/A')} Tênis Detectados")
                st.caption(f"👥 {card_data.get('persons_count', 'N/A')} Pessoas com Demografia")


def render_executive_summary(data):
    st.subheader("📝 Resumo Executivo (Agregado)")
    
    leader_info = data["leader_brand_info"]
    cols = st.columns(3)
    with cols[0]:
        st.metric(label="Marca Líder", value=leader_info["name"], help=f"{leader_info['count']} tênis desta marca encontrados no total.", border=True)
    with cols[1]:
        st.metric(label="Participação da Marca Líder",
                  value=f"{leader_info['percentage']:.1f}%",
                  help=f"{data['unique_brands_count']} marcas diferentes encontradas no total." , border=True)
    with cols[2]:
        st.metric(label="Cobertura Amostral (Pessoas)",
                  value=f"{data['persons_analyzed_count']} Pessoas",
                  help=f"Total de pessoas com dados demográficos analisados nas provas selecionadas.",
                  border=True)

    cols2 = st.columns(2)
    with cols2[0]:
         st.metric(label="Marcas Reconhecidas",
                   value=f"{data['unique_brands_count']} Marcas",
                   help=f"{data['total_shoes_detected']} tênis no total analisados nas provas selecionadas.",
                   border=True)
    with cols2[1]:
        st.metric(label="Margem de Erro",
                  value="+- 5%",
                  help="Margem de erro estimada com base na amostra de dados coletados.",
                  border=True)

    st.markdown("---")
    insight_text = "Nenhum insight gerado (sem dados suficientes)."
    if leader_info["name"] != "N/A" and leader_info['percentage'] > 0:
        insight_text = f"""
        ✨ **Insight-Chave (Agregado)**
        
        A marca **{leader_info["name"]}** demonstra uma presença significativa, capturando **{leader_info['percentage']:.1f}%** do share entre os tênis identificados nas provas selecionadas.
        A análise detalhada por prova e demografia pode revelar nuances importantes.
        """
    st.info(insight_text)
    st.markdown("---")

def render_processing_stats(data):
    st.subheader("⚙️ Estatísticas de Processamento (Agregado)")
    cols = st.columns(3)
    with cols[0]:
        st.metric("Total de Imagens Analisadas", str(data["total_images_selected"]), border=True)
    with cols[1]:
        st.metric("Modelo Utilizado", "Versão AI 3.1 (mock)", help="Acurácia estimada: 92% (mock)", border=True)
    with cols[2]:
        st.metric("Data/Hora Processamento", "04/05/2025 22:00 (mock)", help="Tempo total: 8 horas (mock)", border=True)
    st.markdown("---")

def render_brand_distribution_chart(brand_counts, highlight=None):
    """
    Renders a bar chart showing brand distribution with percentages.
    
    Args:
        brand_counts: Series with brand counts
        highlight: List of brand names to highlight with a different color
    """
    st.subheader("📊 Distribuição de Marcas")
    
    if brand_counts.empty:
        st.caption("Nenhuma marca detectada para gerar o gráfico.")
        return
    
    # Prepare data using the reusable function
    sorted_counts = brand_counts.sort_values(ascending=False)
    total = sorted_counts.sum()
    
    chart_data = pd.DataFrame({
        'Marca': sorted_counts.index,
        'Percentual': (sorted_counts / total * 100).round(1)
    })
    
    # Create highlight condition if needed
    highlight_condition = None
    if highlight and isinstance(highlight, list):
        highlight_brands_str = ', '.join([f'"{brand}"' for brand in highlight])
        highlight_condition = f"indexof([{highlight_brands_str}], datum.Marca) >= 0"
    
    # Use the reusable chart builder
    chart = create_bar_chart(
        data=chart_data,
        x_col='Percentual',
        y_col='Marca',
        title="",
        highlight_condition=highlight_condition
    )
    
    st.altair_chart(chart, use_container_width=True)
        
def render_segmentation_chart(data_dist, title, demographic_col_name_for_legend):
    """
    Renders a grouped bar chart for segmentation (e.g., gender by brand).

    Args:
        data_dist (pd.DataFrame): DataFrame where index is the primary category (e.g., shoe_brand),
                                  columns are the demographic segments (e.g., gender labels),
                                  and values are counts.
                                  Example:
                                    shoe_brand  Masculino  Feminino
                                    Nike             10        15
                                    Adidas            8        12
        title (str): The title for the chart section.
        demographic_col_name_for_legend (str): Name for the demographic column in the legend (e.g., "Gênero").
    """
    st.subheader(title)
    if data_dist is None or data_dist.empty:
        st.caption(f"Não há dados suficientes de {demographic_col_name_for_legend.lower()} e marca para este gráfico.")
        return

    # Reshape data from wide to long format for Altair
    # Input:
    # shoe_brand  Masculino  Feminino
    # Nike             10        15
    # Adidas            8        12
    #
    # Output:
    # shoe_brand demographic_category  count
    # Nike       Masculino             10
    # Nike       Feminino              15
    # Adidas     Masculino              8
    # Adidas     Feminino              12
    
    if data_dist.index.name is None:
        data_dist.index.name = 'Categoria Principal'
    data_long = data_dist.reset_index().melt(
        id_vars=data_dist.index.name, # e.g., 'shoe_brand'
        var_name=demographic_col_name_for_legend, # e.g., 'Gênero'
        value_name='count'
    )

    if data_long.empty or data_long['count'].sum() == 0:
        st.caption(f"Não há dados suficientes de {demographic_col_name_for_legend.lower()} e marca para este gráfico após o processamento.")
        return

    primary_category_name = data_dist.index.name if data_dist.index.name else 'Categoria Principal'

    chart = alt.Chart(data_long).mark_bar().encode(
        x=alt.X(f'{primary_category_name}:N', title=primary_category_name.replace("_", " ").title(), sort=None), # Brand on X-axis
        y=alt.Y('count:Q', title='Contagem'),
        color=alt.Color(f'{demographic_col_name_for_legend}:N', title=demographic_col_name_for_legend.title()),
        column=alt.Column(f'{demographic_col_name_for_legend}:N', title=demographic_col_name_for_legend.title(), header=alt.Header(labelOrient='bottom')), # Creates grouped bars
        tooltip=[
            alt.Tooltip(f'{primary_category_name}:N', title=primary_category_name.replace("_", " ").title()),
            alt.Tooltip(f'{demographic_col_name_for_legend}:N', title=demographic_col_name_for_legend.title()),
            alt.Tooltip('count:Q', title='Contagem')
        ]
    ).properties(
        width=150 # Adjust width per group as needed
    ).configure_facet(
        spacing=10 # Spacing between grouped charts
    )
    # A more common way for grouped bar charts:
    # chart = alt.Chart(data_long).mark_bar().encode(
    #     x=alt.X(f'{demographic_col_name_for_legend}:N', title=demographic_col_name_for_legend.title()),
    #     y=alt.Y('count:Q', title='Contagem'),
    #     color=alt.Color(f'{demographic_col_name_for_legend}:N', title=demographic_col_name_for_legend.title()), # Optional: if you want different colors for bars within the same group
    #     column=alt.Column(f'{primary_category_name}:N', title=primary_category_name.replace("_", " ").title()) # Facet by brand
    # ).properties(
    #     # width=alt.Step(40) # Adjust width of bars if needed
    # )


    # Simpler Grouped Bar Chart (often preferred)
    grouped_chart = alt.Chart(data_long).mark_bar().encode(
        # X-axis: Primary category (e.g., shoe brand)
        x=alt.X(f'{primary_category_name}:N', title=primary_category_name.replace("_", " ").title(), sort=None),
        # Y-axis: Count
        y=alt.Y('count:Q', title='Contagem'),
        # Color encodes the demographic category (e.g., gender), creating the groups
        color=alt.Color(f'{demographic_col_name_for_legend}:N', title=demographic_col_name_for_legend.title()),
        # X-offset for grouped bars
        xOffset=f'{demographic_col_name_for_legend}:N',
        tooltip=[
            alt.Tooltip(f'{primary_category_name}:N', title=primary_category_name.replace("_", " ").title()),
            alt.Tooltip(f'{demographic_col_name_for_legend}:N', title=demographic_col_name_for_legend.title()),
            alt.Tooltip('count:Q', title='Contagem', format=',d')
        ]
    ).configure_legend(
        orient='top'
    )

    st.altair_chart(grouped_chart, use_container_width=True)

def render_marathon_comparison_chart(brand_counts_by_marathon, highlight=None):
    """
    Renders comparison charts for brand distribution across marathons.
    Refactored to use reusable chart components.
    """
    st.subheader("🏁 Marcas por Prova (Comparativo)")
    
    if brand_counts_by_marathon.empty:
        st.caption("Não há dados de marcas por prova/pasta para este gráfico.")
        return
    
    for marathon_name, counts in brand_counts_by_marathon.iterrows():
        if counts.sum() == 0:
            st.caption(f"Não há dados de marcas para a prova '{marathon_name}'.")
            continue
            
        # Prepare data for the chart
        marathon_data = pd.DataFrame({
            'Marca': counts.index,
            'Contagem': counts.values,
            'Percentual': (counts / counts.sum() * 100).round(1)
        })
        
        # Filter out zero counts and sort
        marathon_data = marathon_data[marathon_data['Contagem'] > 0].sort_values('Contagem', ascending=False)
        
        if marathon_data.empty:
            st.caption(f"Não há marcas detectadas para a prova '{marathon_name}'.")
            continue
        
        # Create highlight condition
        highlight_condition = None
        if highlight and isinstance(highlight, list):
            highlight_brands_str = ', '.join([f'"{brand}"' for brand in highlight])
            highlight_condition = f"indexof([{highlight_brands_str}], datum.Marca) >= 0"
        
        # Use the reusable chart builder
        chart = create_bar_chart(
            data=marathon_data,
            x_col='Percentual',
            y_col='Marca',
            title=f"{marathon_name}",
            highlight_condition=highlight_condition
        )
        
        st.altair_chart(chart, use_container_width=True)
        
        # Add separator between marathons (except for the last one)
        if marathon_name != brand_counts_by_marathon.index[-1]:
            st.markdown("---")

def render_top_brands_table(top_brands_df):
    st.subheader("👟 Top Marcas de Tênis (Agregado nas Provas Selecionadas)")
    if not top_brands_df.empty:
        st.dataframe(top_brands_df, use_container_width=True, hide_index=True)
    else:
        st.caption("Nenhuma marca detectada para exibir o top.")


def render_individual_marathon_column(marathon_name: str, marathon_data: Dict[str, Any]) -> None:
    """
    Render a single marathon's data in a column with organized sections.
    
    Args:
        marathon_name: Name of the marathon
        marathon_data: Processed data for the marathon
    """
    st.subheader(f"📊 {marathon_name}")
    
    # Marathon info card
    marathon_cards_data = {
        marathon_name: marathon_data.get("marathon_specific_data_for_cards", {}).get(marathon_name, {})
    }
    
    render_marathon_info_cards(
        [marathon_name], 
        marathon_cards_data,
        st.session_state.get("MARATHON_OPTIONS_DB_CACHED", [])
    )
    
    # Check if there's meaningful data
    has_brand_data = not marathon_data["brand_counts_all_selected"].empty
    has_gender_data = not marathon_data["gender_brand_distribution"].empty
    has_race_data = not marathon_data["race_brand_distribution"].empty
    
    if not has_brand_data:
        st.info("📋 Nenhum dado de marcas disponível para esta prova.")
        return
    
    # Brand distribution
    with st.expander("📊 Distribuição de Marcas", expanded=True):
        render_brand_distribution_chart(
            marathon_data["brand_counts_all_selected"],
            highlight=marathon_data.get("highlight_brands", ["Olympikus", "Mizuno"])
        )
    
    # Gender analysis
    if has_gender_data:
        with st.expander("👥 Análise por Gênero"):
            render_gender_by_brand(marathon_data["gender_brand_distribution"], min_percentage_for_display=5.0)
    
    # Race analysis
    if has_race_data:
        with st.expander("🌍 Análise por Raça"):
            render_race_by_brand(marathon_data["race_brand_distribution"], min_percentage_for_display=5.0)
    
    # Category analysis (for single marathon showing different categories within it)
    has_category_data = not marathon_data.get("brand_counts_by_category", pd.DataFrame()).empty
    if has_category_data:
        with st.expander("📏 Análise por Categoria"):
            render_category_comparison_chart(
                marathon_data["brand_counts_by_category"], 
                highlight=marathon_data.get("highlight_brands", ["Olympikus", "Mizuno"])
            )
    
    # Top brands table
    with st.expander("🏆 Top Marcas"):
        render_top_brands_table(marathon_data["top_brands_all_selected"])


# --- Main Report Function (Updated to focus on individual columns) ---

def report_page_content_main(processed_metrics: Dict[str, Any], marathon_specific_data_for_cards: Dict[str, Any]) -> None:
    """
    Main function to render marathon reports in individual columns only.
    
    Args:
        processed_metrics: Dictionary of calculated metrics
        marathon_specific_data_for_cards: Marathon-specific data for cards
    """
    if processed_metrics.get("total_images_selected", 0) == 0:
        st.warning("Nenhuma imagem nos dados selecionados para gerar o relatório.")
        return
    
    selected_marathon_names = list(processed_metrics.get("marathon_specific_data_for_cards", {}).keys())
    
    if not selected_marathon_names:
        st.warning("Nenhuma prova selecionada.")
        return
    
    # Always display marathons in individual columns
    cols = st.columns(len(selected_marathon_names))
    
    for i, marathon_name in enumerate(selected_marathon_names):
        with cols[i]:
            render_individual_marathon_column(marathon_name, processed_metrics)


def render_pdf_preview_modal(processed_metrics, marathon_specific_data_for_cards):
    st.caption(f"Total de páginas: X (estimado)")
    with st.container(height=600):
            report_page_content_main(processed_metrics, marathon_specific_data_for_cards)
    st.markdown("---")
    btn_cols = st.columns([1,5,1])
    with btn_cols[0]:
        if st.button("Exportar Relatório (PDF - Mock)", type="primary", use_container_width=True, key="pdf_export_confirm_modal_db"):
            st.toast("Funcionalidade de exportação PDF não implementada.")
            st.session_state.show_pdf_preview_db = False
            st.rerun()
    with btn_cols[2]:
        if st.button("Fechar Preview", use_container_width=True, key="pdf_close_preview_modal_db"):
            st.session_state.show_pdf_preview_db = False
            st.rerun()

def render_demographic_by_brand_chart(
    demographic_data: pd.DataFrame,
    demographic_type: str,
    min_percentage: float = 5.0,
    color_scheme: Optional[Dict[str, str]] = None
) -> None:
    """
    Render a demographic breakdown by brand stacked bar chart.
    
    Args:
        demographic_data: DataFrame with demographic data
        demographic_type: Type of demographic (e.g., "Gênero", "Raça")
        min_percentage: Minimum percentage for individual brand display
        color_scheme: Optional color mapping for demographics
    """
    icon_map = {"Gênero": "🚻", "Raça": "🌍"}
    icon = icon_map.get(demographic_type, "📊")
    
    st.subheader(f"{icon} Contagem de Tênis por Marca e {demographic_type}")
    
    if demographic_data is None or demographic_data.empty:
        st.caption(f"Não há dados de {demographic_type.lower()} e marca para exibir.")
        return
    
    chart_data = prepare_demographic_data_for_chart(demographic_data, min_percentage)
    
    if chart_data.empty:
        st.caption(f"Não há dados processados de {demographic_type.lower()} e marca para exibir.")
        return
    
    # Get sorted brand order based on totals
    brand_sort_order = chart_data.groupby('shoe_brand')['brand_total'].first().sort_values(ascending=False).index.tolist()
    
    # Create color encoding
    color_encoding = (
        alt.Color('demographic_category:N', 
                 title=demographic_type,
                 scale=alt.Scale(domain=list(color_scheme.keys()), 
                               range=list(color_scheme.values())))
        if color_scheme else alt.Color('demographic_category:N', title=demographic_type)
    )
    
    # Create the stacked bar chart
    chart = alt.Chart(chart_data).mark_bar().encode(
        y=alt.Y('shoe_brand:N', 
               title='Marca',
               sort=brand_sort_order,
               axis=alt.Axis(labelLimit=200)),
        x=alt.X('percentage:Q',
               axis=alt.Axis(format='%'),
               title='Percentual',
               stack=True),
        color=color_encoding,
        order=alt.Order('demographic_category:N', sort='descending'),
        tooltip=[
            alt.Tooltip('shoe_brand:N', title='Marca'),
            alt.Tooltip('demographic_category:N', title=demographic_type),
            alt.Tooltip('percentage:Q', title='Percentual na Marca', format='.1%')
        ]
    ).properties(height=max(400, len(brand_sort_order) * 30))
    
    st.altair_chart(chart, use_container_width=True)
    st.caption(f"Marcas com menos de {min_percentage}% do total foram agrupadas como 'Outros'.")


def render_gender_by_brand(gender_brand_data: pd.DataFrame, min_percentage_for_display: float = 2.0) -> None:
    """Render gender breakdown by brand chart."""
    color_scheme = {"male": "#1f77b4", "female": "#990785"}
    render_demographic_by_brand_chart(
        gender_brand_data, 
        "Gênero", 
        min_percentage_for_display,
        color_scheme
    )


def render_race_by_brand(race_brand_data: pd.DataFrame, min_percentage_for_display: float = 2.0) -> None:
    """Render race breakdown by brand chart."""
    render_demographic_by_brand_chart(
        race_brand_data, 
        "Raça", 
        min_percentage_for_display
    )


def render_brand_timeline_chart(timeline_data: pd.DataFrame) -> None:
    """
    Render a line chart showing brand percentage evolution over time.
    
    Args:
        timeline_data: DataFrame with columns: marathon_name, event_date, brand, percentage
    """
    st.subheader("📈 Evolução da Participação das Marcas ao Longo do Tempo")
    
    if timeline_data.empty:
        st.warning("Não há dados temporais disponíveis para visualização.")
        return
    
    # Get top brands to focus on (top 8 to avoid clutter)
    top_brands = timeline_data.groupby('brand')['percentage'].mean().sort_values(ascending=False).head(8).index.tolist()
    
    # Filter data to top brands only
    filtered_data = timeline_data[timeline_data['brand'].isin(top_brands)].copy()
    
    if filtered_data.empty:
        st.warning("Não há dados suficientes para gerar o gráfico temporal.")
        return
    
    # Create color palette for brands
    color_scale = alt.Scale(scheme='category20')
    
    # Create the line chart with points
    chart = alt.Chart(filtered_data).mark_line(
        point=True,
        strokeWidth=3
    ).encode(
        x=alt.X('event_date:T', 
                title='Data da Prova',
                axis=alt.Axis(format='%b %Y', labelAngle=-45)),
        y=alt.Y('percentage:Q', 
                title='Participação (%)',
                scale=alt.Scale(domain=[0, filtered_data['percentage'].max() * 1.1])),
        color=alt.Color('brand:N', 
                       title='Marca',
                       scale=color_scale),
        tooltip=[
            alt.Tooltip('marathon_name:N', title='Prova'),
            alt.Tooltip('event_date:T', title='Data', format='%d/%m/%Y'),
            alt.Tooltip('brand:N', title='Marca'),
            alt.Tooltip('percentage:Q', title='Participação (%)', format='.1f'),
            alt.Tooltip('count:Q', title='Contagem')
        ]
    ).properties(
        height=400,
        title=alt.TitleParams(
            text="Evolução da Participação das Marcas por Prova",
            anchor='start'
        )
    ).resolve_scale(
        color='independent'
    )
    
    st.altair_chart(chart, use_container_width=True)
    
    # Add summary insights
    st.markdown("---")
    render_timeline_insights(filtered_data, top_brands)

def render_timeline_insights(timeline_data: pd.DataFrame, top_brands: list) -> None:
    """
    Render insights about brand evolution over time.
    
    Args:
        timeline_data: DataFrame with timeline data
        top_brands: List of top brand names
    """
    st.subheader("💡 Insights da Evolução Temporal")
    
    if timeline_data.empty or len(timeline_data['event_date'].unique()) < 2:
        st.info("São necessárias pelo menos 2 provas com datas para gerar insights temporais.")
        return
    
    insights = []
    
    # Calculate trends for each brand
    for brand in top_brands:
        brand_data = timeline_data[timeline_data['brand'] == brand].sort_values('event_date')
        
        if len(brand_data) < 2:
            continue
            
        # Calculate trend (simple: compare first and last values)
        first_percentage = brand_data.iloc[0]['percentage']
        last_percentage = brand_data.iloc[-1]['percentage']
        change = last_percentage - first_percentage
        
        if abs(change) > 2:  # Only show significant changes
            trend = "crescimento" if change > 0 else "queda"
            insights.append({
                'brand': brand,
                'trend': trend,
                'change': abs(change),
                'direction': '📈' if change > 0 else '📉'
            })
    
    if insights:
        # Sort by magnitude of change
        insights.sort(key=lambda x: x['change'], reverse=True)
        
        # Display top insights
        for i, insight in enumerate(insights[:3]):  # Show top 3 insights
            if i == 0:
                st.markdown(f"""
                **{insight['direction']} Destaque Principal:** A marca **{insight['brand']}** apresentou {insight['trend']} 
                de **{insight['change']:.1f} pontos percentuais** entre a primeira e última prova analisada.
                """)
            else:
                st.markdown(f"""
                • **{insight['brand']}**: {insight['trend']} de {insight['change']:.1f}pp {insight['direction']}
                """)
    else:
        st.info("As marcas mantiveram participações relativamente estáveis ao longo do tempo.")
    
    # Show data coverage info
    total_marathons = len(timeline_data['marathon_name'].unique())
    date_range = timeline_data['event_date'].agg(['min', 'max'])
    
    st.caption(f"""
    📊 **Cobertura dos Dados:** {total_marathons} provas analisadas 
    de {date_range['min'].strftime('%b/%Y')} até {date_range['max'].strftime('%b/%Y')}
    """)

def render_category_comparison_chart(brand_counts_by_category, highlight=None):
    """
    Renders comparison charts for brand distribution across categories (5km, 10km, 21km, etc.).
    
    Args:
        brand_counts_by_category: DataFrame where index is category and columns are brands
        highlight: List of brand names to highlight with a different color
    """
    st.subheader("🏃‍♂️ Marcas por Categoria (Comparativo)")
    
    if brand_counts_by_category.empty:
        st.caption("Não há dados de marcas por categoria para este gráfico.")
        return
    
    for category_name, counts in brand_counts_by_category.iterrows():
        if counts.sum() == 0:
            st.caption(f"Não há dados de marcas para a categoria '{category_name}'.")
            continue
            
        # Prepare data for the chart
        category_data = pd.DataFrame({
            'Marca': counts.index,
            'Contagem': counts.values,
            'Percentual': (counts / counts.sum() * 100).round(1)
        })
        
        # Filter out zero counts and sort
        category_data = category_data[category_data['Contagem'] > 0].sort_values('Contagem', ascending=False)
        
        if category_data.empty:
            st.caption(f"Não há marcas detectadas para a categoria '{category_name}'.")
            continue
        
        # Create highlight condition
        highlight_condition = None
        if highlight and isinstance(highlight, list):
            highlight_brands_str = ', '.join([f'"{brand}"' for brand in highlight])
            highlight_condition = f"indexof([{highlight_brands_str}], datum.Marca) >= 0"
        
        # Use the reusable chart builder
        chart = create_bar_chart(
            data=category_data,
            x_col='Percentual',
            y_col='Marca',
            title=f"Categoria: {category_name}",
            highlight_condition=highlight_condition
        )
        
        st.altair_chart(chart, use_container_width=True)
        
        # Add separator between categories (except for the last one)
        if category_name != brand_counts_by_category.index[-1]:
            st.markdown("---")

def render_category_timeline_chart(timeline_data: pd.DataFrame) -> None:
    """
    Render a line chart showing brand percentage evolution across different categories.
    
    Args:
        timeline_data: DataFrame with columns: marathon_name, event_date, category, brand, percentage
    """
    st.subheader("📈 Evolução das Marcas por Categoria")
    
    if timeline_data.empty:
        st.warning("Não há dados temporais disponíveis para visualização por categoria.")
        return
    
    # Check if we have multiple categories
    categories = timeline_data['category'].unique()
    if len(categories) < 2:
        st.warning("São necessárias pelo menos 2 categorias para gerar o gráfico temporal.")
        return
    
    # Get top brands to focus on (top 8 to avoid clutter)
    top_brands = timeline_data.groupby('brand')['percentage'].mean().sort_values(ascending=False).head(8).index.tolist()
    
    # Filter data to top brands only
    filtered_data = timeline_data[timeline_data['brand'].isin(top_brands)].copy()
    
    if filtered_data.empty:
        st.warning("Não há dados suficientes para gerar o gráfico temporal por categoria.")
        return
    
    # Create color palette for brands
    color_scale = alt.Scale(scheme='category20')
    
    # Create the line chart with points
    chart = alt.Chart(filtered_data).mark_line(
        point=True,
        strokeWidth=3
    ).encode(
        x=alt.X('category:N', 
                title='Categoria',
                sort=categories.tolist()),
        y=alt.Y('percentage:Q', 
                title='Participação (%)',
                scale=alt.Scale(domain=[0, filtered_data['percentage'].max() * 1.1])),
        color=alt.Color('brand:N', 
                       title='Marca',
                       scale=color_scale),
        tooltip=[
            alt.Tooltip('marathon_name:N', title='Prova'),
            alt.Tooltip('category:N', title='Categoria'),
            alt.Tooltip('brand:N', title='Marca'),
            alt.Tooltip('percentage:Q', title='Participação (%)', format='.1f'),
            alt.Tooltip('count:Q', title='Contagem')
        ]
    ).properties(
        height=400,
        title=alt.TitleParams(
            text="Evolução da Participação das Marcas por Categoria",
            anchor='start'
        )
    ).resolve_scale(
        color='independent'
    )
    
    st.altair_chart(chart, use_container_width=True)
    
    # Add summary insights
    st.markdown("---")
    render_category_timeline_insights(filtered_data, top_brands, categories.tolist())

def render_category_timeline_insights(timeline_data: pd.DataFrame, top_brands: list, categories: list) -> None:
    """
    Render insights about brand evolution across categories.
    
    Args:
        timeline_data: DataFrame with timeline data
        top_brands: List of top brand names
        categories: List of categories
    """
    st.subheader("💡 Insights da Evolução por Categoria")
    
    if timeline_data.empty or len(categories) < 2:
        st.info("São necessárias pelo menos 2 categorias para gerar insights.")
        return
    
    insights = []
    
    # Calculate trends for each brand across categories
    for brand in top_brands:
        brand_data = timeline_data[timeline_data['brand'] == brand].copy()
        
        if len(brand_data) < 2:
            continue
        
        # Sort by category (assuming categories are in order like 5km, 10km, 21km)
        brand_data = brand_data.sort_values('category')
        
        # Calculate trend (compare shortest to longest distance if possible)
        if len(brand_data) >= 2:
            first_percentage = brand_data.iloc[0]['percentage']
            last_percentage = brand_data.iloc[-1]['percentage']
            change = last_percentage - first_percentage
            
            if abs(change) > 2:  # Only show significant changes
                trend = "crescimento" if change > 0 else "queda"
                first_category = brand_data.iloc[0]['category']
                last_category = brand_data.iloc[-1]['category']
                
                insights.append({
                    'brand': brand,
                    'trend': trend,
                    'change': abs(change),
                    'direction': '📈' if change > 0 else '📉',
                    'first_category': first_category,
                    'last_category': last_category
                })
    
    if insights:
        # Sort by magnitude of change
        insights.sort(key=lambda x: x['change'], reverse=True)
        
        # Display top insights
        for i, insight in enumerate(insights[:3]):  # Show top 3 insights
            if i == 0:
                st.markdown(f"""
                **{insight['direction']} Destaque Principal:** A marca **{insight['brand']}** apresentou {insight['trend']} 
                de **{insight['change']:.1f} pontos percentuais** da categoria {insight['first_category']} para {insight['last_category']}.
                """)
            else:
                st.markdown(f"""
                • **{insight['brand']}**: {insight['trend']} de {insight['change']:.1f}pp ({insight['first_category']} → {insight['last_category']}) {insight['direction']}
                """)
    else:
        st.info("As marcas mantiveram participações relativamente estáveis entre as categorias.")
    
    # Show data coverage info
    total_marathons = len(timeline_data['marathon_name'].unique())
    
    st.caption(f"""
    📊 **Cobertura dos Dados:** {len(categories)} categorias analisadas em {total_marathons} provas: {', '.join(categories)}
    """)