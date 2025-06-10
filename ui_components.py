import streamlit as st
import pandas as pd
import altair as alt # Make sure altair is imported
import math

def create_column_grid(num_items: int, items_per_row: int):
    """
    Creates a grid of Streamlit columns and returns them as a flat list.

    Args:
        num_items (int): The total number of items to display.
        items_per_row (int): The maximum number of items (columns) per visual row.

    Returns:
        list: A flat list of Streamlit column objects.
    """
    if num_items <= 0 or items_per_row <= 0:
        return []

    flat_columns_list = []
    num_rows = math.ceil(num_items / items_per_row)

    for i in range(num_rows):
        start_index = i * items_per_row
        # Determine how many columns are needed for the current row
        num_cols_in_this_row = min(items_per_row, num_items - start_index)
        
        if num_cols_in_this_row > 0:
            # st.columns(N) creates N equally sized columns
            row_cols = st.columns(num_cols_in_this_row)
            flat_columns_list.extend(row_cols)
            
    return flat_columns_list

# --- UI Components for Main App ---

def logout_button(key_suffix="", button_text="🚪 Sair", help_text="Fazer logout"):
    """
    Reusable logout button component that clears session state and redirects to login.
    
    Args:
        key_suffix (str): Suffix to add to the button key to ensure uniqueness
        button_text (str): Text to display on the button
        help_text (str): Tooltip text for the button
    
    Returns:
        bool: True if logout button was clicked, False otherwise
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
                st.subheader(marathon_name)
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
        brand_counts (pd.DataFrame): DataFrame containing brand count data
        highlight (list, optional): List of brand names to highlight with a different color
    """
    st.subheader("📊 Distribuição de Marcas (Global nas Provas Selecionadas)")
    if not brand_counts.empty:
        # Sort by value descending (higher to lower share)
        sorted_counts = brand_counts.sort_values(ascending=False)
        
        # Calculate percentages
        total = sorted_counts.sum()
        percentages = (sorted_counts / total * 100).round(1)
        
        # Create a DataFrame for Altair
        chart_data = pd.DataFrame({
            'Marca': percentages.index,
            'Percentual': percentages.values
        })
        
        # Create color condition based on highlight parameter
        if highlight and isinstance(highlight, list):
            # Create a condition using a proper Vega expression 
            # Use a test expression to check if the current brand is in the highlight list
            highlight_brands_str = ', '.join([f'"{brand}"' for brand in highlight])
            color_condition = alt.condition(
                f"indexof([{highlight_brands_str}], datum.Marca) >= 0",  # Vega expression to check if in list
                alt.value('#ff6b6b'),  # highlighted color
                alt.value('#1f77b4')   # default color
            )
        else:
            color_condition = alt.value('#1f77b4')  # default color for all
        
        # Create the bar chart with Altair
        chart = alt.Chart(chart_data).mark_bar().encode(
            y=alt.Y('Marca:N', sort='-x', title='Marca'),
            x=alt.X('Percentual:Q', title='Percentual (%)', scale=alt.Scale(domain=[0, 50])),
            color=color_condition,
            tooltip=[
                alt.Tooltip('Marca:N', title='Marca'),
                alt.Tooltip('Percentual:Q', title='Percentual (%)', format='.1f')
            ]
        ).properties(
            height=400
        )
        
        st.altair_chart(chart, use_container_width=True)
    else:
        st.caption("Nenhuma marca detectada para gerar o gráfico.")
        
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
    st.subheader("🏁 Marcas por Prova (Comparativo)")
    #iterate on each index and create a bar chart for each marathon displaying the brands with their percentage
    if not brand_counts_by_marathon.empty:
        for marathon_name, counts in brand_counts_by_marathon.iterrows():
            total_count = counts.sum()
            if total_count == 0:
                st.caption(f"Não há dados de marcas para a prova '{marathon_name}'.")
                continue
            # Create a DataFrame for the current marathon
            marathon_data = pd.DataFrame({
                'Marca': counts.index,
                'Contagem': counts.values,
                'Percentual': (counts / total_count * 100).round(1)
            }).reset_index(drop=True)
            # Filter out zero counts
            marathon_data = marathon_data[marathon_data['Contagem'] > 0]
            if marathon_data.empty:
                st.caption(f"Não há marcas detectadas para a prova '{marathon_name}'.")
                continue
            # Create the bar chart for this marathon
            # Sort by count descending
            marathon_data = marathon_data.sort_values(by='Contagem', ascending=False)

    
            if highlight and isinstance(highlight, list):
                # Create a condition using a proper Vega expression 
                # Use a test expression to check if the current brand is in the highlight list
                highlight_brands_str = ', '.join([f'"{brand}"' for brand in highlight])
                color_condition = alt.condition(
                    f"indexof([{highlight_brands_str}], datum.Marca) >= 0",  # Vega expression to check if in list
                    alt.value('#ff6b6b'),  # highlighted color
                    alt.value('#1f77b4')   # default color
                )
            else:
                color_condition = alt.value('#1f77b4')  # default color for all
        
            chart = alt.Chart(marathon_data).mark_bar().encode(
                y=alt.Y('Marca:N', title='Marca', sort='-x'),
                x=alt.X('Percentual:Q', title='Percentual (%)', scale=alt.Scale(domain=[0, 50])),
                color=color_condition,
                tooltip=[
                    alt.Tooltip('Marca:N', title='Marca'),
                    alt.Tooltip('Percentual:Q', title='Percentual (%)', format='.1f')
                ]
            ).properties(
                title=f"{marathon_name}",
                height=400
            )
            st.altair_chart(chart, use_container_width=True)
            st.caption(f"Prova {marathon_name}")

            if marathon_name != brand_counts_by_marathon.index[-1]:
                st.markdown("---")
    else:
         st.caption("Não há dados de marcas por prova/pasta para este gráfico.")

def render_top_brands_table(top_brands_df):
    st.subheader("👟 Top Marcas de Tênis (Agregado nas Provas Selecionadas)")
    if not top_brands_df.empty:
        st.dataframe(top_brands_df, use_container_width=True, hide_index=True)
    else:
        st.caption("Nenhuma marca detectada para exibir o top.")


# --- Report Page Content (Main function to call others) ---
def report_page_content_main(processed_metrics, marathon_specific_data_for_cards): # marathon_specific_data_for_cards is now redundant if it's inside processed_metrics
    """
    Main function to render all sections of the report.
    processed_metrics: Dictionary of calculated metrics.
    """
    if processed_metrics.get("total_images_selected", 0) == 0: # Check if key exists
        st.warning("Nenhuma imagem nos dados selecionados para gerar o relatório.")
        return
    
    # Get the list of selected marathon names from the keys of marathon_specific_data_for_cards
    selected_marathon_names = list(processed_metrics.get("marathon_specific_data_for_cards", {}).keys())
    
    # Fetch full marathon metadata for cards (this might be cached upstream in the page)
    # For simplicity, assuming MARATHON_OPTIONS_DB_CACHED is available in session_state if needed,
    # or better, pass it down. For now, we'll use what's in processed_metrics.
    
    render_marathon_info_cards(
        selected_marathon_names, 
        processed_metrics.get("marathon_specific_data_for_cards", {}),
        st.session_state.get("MARATHON_OPTIONS_DB_CACHED", []) # Pass the cached list of marathon metadata
    )
    st.markdown("---")
    render_executive_summary(processed_metrics)
    render_processing_stats(processed_metrics)
    render_brand_distribution_chart(
        processed_metrics["brand_counts_all_selected"],
        highlight=processed_metrics.get("highlight_brands", ["Olympikus", "Mizuno"])
    )
    
    st.markdown("---")
    render_gender_by_brand(processed_metrics["gender_brand_distribution"], min_percentage_for_display=5.0)
    
    st.markdown("---")
    render_race_by_brand(processed_metrics["race_brand_distribution"], min_percentage_for_display=5.0)

    st.markdown("---")
    render_marathon_comparison_chart(processed_metrics["brand_counts_by_marathon"],
                                             highlight=processed_metrics.get("highlight_brands", ["Olympikus", "Mizuno"]))

    st.markdown("---")
    render_top_brands_table(processed_metrics["top_brands_all_selected"])


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

def render_gender_by_brand(gender_brand_data: pd.DataFrame, min_percentage_for_display=2.0):
    """
    Renders a horizontal stacked barplot showing the gender breakdown for each brand.
    
    Args:
        gender_brand_data (pd.DataFrame): DataFrame where index is 'shoe_brand',
                                          columns are gender categories (e.g., 'Masculino', 'Feminino'),
                                          and values are absolute counts.
        min_percentage_for_display (float): Minimum percentage a brand must have to be displayed individually.
                                            Brands below this threshold will be grouped as 'Outros'.
    """
    st.subheader("🚻 Contagem de Tênis por Marca e Gênero")

    if gender_brand_data is None or gender_brand_data.empty:
        st.caption("Não há dados de gênero e marca para exibir.")
        return

    # Make a copy to avoid modifying the original DataFrame
    data = gender_brand_data.copy()
    data = data.T
    
    # Ensure index is named
    if data.index.name is None:
        data.index.name = "shoe_brand"
    brand_col_name = data.index.name

    # Calculate total counts for each brand
    data['total'] = data.sum(axis=1)
    total_shoes = data['total'].sum()
    
    # Calculate percentage for each brand
    data['percentage'] = (data['total'] / total_shoes) * 100
    
    # Identify brands to be included in 'Outros'
    small_brands = data[data['percentage'] < min_percentage_for_display].index
    
    # If we have small brands, group them into 'Outros'
    if len(small_brands) > 0:
        # Create a new row 'Outros' with the sum of all small brands
        others_row = data.loc[small_brands].sum()
        
        # Remove small brands from the original DataFrame
        data = data.drop(small_brands)
        
        # Add the 'Outros' row
        data.loc['Outros'] = others_row
    
    # Sort by total count (descending)
    
    # Remove helper columns before converting to long format
    data = data.drop(['total', 'percentage'], axis=1)
    
    # Melt the DataFrame to long format for Altair
    df_long = data.reset_index().melt(
        id_vars=brand_col_name,
        var_name="gender",
        value_name="count"
    )
    
    if df_long.empty or df_long['count'].sum() == 0:
        st.caption("Não há dados processados de gênero e marca para exibir.")
        return
    
    # Calculate percentage within each brand for stacked bars
    df_long_percent = df_long.copy()
    # Add total count per brand for calculations
    brand_totals = df_long.groupby(brand_col_name)['count'].sum().reset_index()
    brand_totals.columns = [brand_col_name, 'brand_total']
    
    # Merge to get the brand totals
    df_long_percent = pd.merge(df_long_percent, brand_totals, on=brand_col_name)
    # Calculate percentage within brand
    df_long_percent['percentage'] = df_long_percent['count'] / df_long_percent['brand_total']
    
    # Get sorted brand order based on totals
    brand_sort_order = brand_totals.sort_values('brand_total', ascending=False)[brand_col_name].tolist()
    
    # Create a horizontal stacked bar chart for gender distribution
    bar_chart = alt.Chart(df_long_percent).mark_bar().encode(
        # Y-axis: brands ordered by total count
        y=alt.Y(f'{brand_col_name}:N', 
                title='Marca',
                sort=brand_sort_order,
                axis=alt.Axis(labelLimit=200)),  # Ensure brand labels are visible
        
        # X-axis: percentage of counts stacked
        x=alt.X('percentage:Q',
                axis=alt.Axis(format='%'),
                title='Contagem',
                stack=True),# Stack the values
        
        # Color by gender
        color=alt.Color('gender:N', 
                        title='Gênero',
                        scale=alt.Scale(
                            domain=["male", "female"],  # Ensure consistent color mapping
                            range=["#1f77b4", "#990785"]  # Custom colors for a
                            # consistent look
                        )),

        
        # Order of stacking (consistent across brands)
        order=alt.Order('gender:N', sort='descending'),
        
        # Tooltips for interactive exploration
        tooltip=[
            alt.Tooltip(f'{brand_col_name}:N', title='Marca'),
            alt.Tooltip('gender:N', title='Gênero'),
            alt.Tooltip('percentage:Q', title='Percentual na Marca', format='.1%')
        ]
    )
    
    # Create a text layer to show total counts at the end of each bar
    
    # Combine the two charts
    chart = (bar_chart).properties(
        # Set appropriate height based on number of brands
        height=max(400, len(brand_sort_order) * 30)
    )
    
    st.altair_chart(chart, use_container_width=True)
    st.caption("As barras mostram a contagem de tênis por gênero para cada marca. Marcas com menos de " + 
              f"{min_percentage_for_display}% do total foram agrupadas como 'Outros'.")
    
def render_race_by_brand(race_brand_data: pd.DataFrame, min_percentage_for_display=2.0):
    """
    Renders a horizontal stacked barplot showing the race breakdown for each brand.
    
    Args:
        race_brand_data (pd.DataFrame): DataFrame where index is 'shoe_brand',
                                          columns are race categories,
                                          and values are absolute counts.
        min_percentage_for_display (float): Minimum percentage a brand must have to be displayed individually.
                                            Brands below this threshold will be grouped as 'Outros'.
    """
    st.subheader("🏃‍♂️ Contagem de Tênis por Marca e Raça")

    if race_brand_data is None or race_brand_data.empty:
        st.caption("Não há dados de raça e marca para exibir.")
        return

    # Make a copy to avoid modifying the original DataFrame
    data = race_brand_data.copy()
    data = data.T
    
    # Ensure index is named
    if data.index.name is None:
        data.index.name = "shoe_brand"
    brand_col_name = data.index.name

    # Calculate total counts for each brand
    data['total'] = data.sum(axis=1)
    total_shoes = data['total'].sum()
    
    # Calculate percentage for each brand
    data['percentage'] = (data['total'] / total_shoes) * 100
    
    # Identify brands to be included in 'Outros'
    small_brands = data[data['percentage'] < min_percentage_for_display].index
    
    # If we have small brands, group them into 'Outros'
    if len(small_brands) > 0:
        # Create a new row 'Outros' with the sum of all small brands
        others_row = data.loc[small_brands].sum()
        
        # Remove small brands from the original DataFrame
        data = data.drop(small_brands)
        
        # Add the 'Outros' row
        data.loc['Outros'] = others_row
    
    # Sort by total count (descending)
    
    # Remove helper columns before converting to long format
    data = data.drop(['total', 'percentage'], axis=1)
    
    # Melt the DataFrame to long format for Altair
    df_long = data.reset_index().melt(
        id_vars=brand_col_name,
        var_name="race",
        value_name="count"
    )
    
    if df_long.empty or df_long['count'].sum() == 0:
        st.caption("Não há dados processados de gênero e marca para exibir.")
        return
    
    # Calculate percentage within each brand for stacked bars
    df_long_percent = df_long.copy()
    # Add total count per brand for calculations
    brand_totals = df_long.groupby(brand_col_name)['count'].sum().reset_index()
    brand_totals.columns = [brand_col_name, 'brand_total']
    
    # Merge to get the brand totals
    df_long_percent = pd.merge(df_long_percent, brand_totals, on=brand_col_name)
    # Calculate percentage within brand
    df_long_percent['percentage'] = df_long_percent['count'] / df_long_percent['brand_total']
    
    # Get sorted brand order based on totals
    brand_sort_order = brand_totals.sort_values('brand_total', ascending=False)[brand_col_name].tolist()
    
    # Create a horizontal stacked bar chart for gender distribution
    bar_chart = alt.Chart(df_long_percent).mark_bar().encode(
        # Y-axis: brands ordered by total count
        y=alt.Y(f'{brand_col_name}:N', 
                title='Marca',
                sort=brand_sort_order,
                axis=alt.Axis(labelLimit=200)),  # Ensure brand labels are visible
        
        # X-axis: percentage of counts stacked
        x=alt.X('percentage:Q',
                axis=alt.Axis(format='%'),
                title='Contagem',
                stack=True),# Stack the values
        
        # Color by gender
        color=alt.Color('race:N', 
                        title='Raça'),

        
        # Order of stacking (consistent across brands)
        order=alt.Order('race:N', sort='descending'),
        
        # Tooltips for interactive exploration
        tooltip=[
            alt.Tooltip(f'{brand_col_name}:N', title='Marca'),
            alt.Tooltip('race:N', title='Raça'),
            alt.Tooltip('percentage:Q', title='Percentual na Marca', format='.1%')
        ]
    )
    
    # Create a text layer to show total counts at the end of each bar
    
    # Combine the two charts
    chart = (bar_chart).properties(
        # Set appropriate height based on number of brands
        height=max(400, len(brand_sort_order) * 30)
    )
    
    st.altair_chart(chart, use_container_width=True)
    st.caption("As barras mostram a contagem de tênis por raça para cada marca. Marcas com menos de " + 
              f"{min_percentage_for_display}% do total foram agrupadas como 'Outros'.")