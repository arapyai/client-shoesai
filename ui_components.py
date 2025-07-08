import streamlit as st
import pandas as pd
import altair as alt
import math
from typing import Optional, List, Dict, Any
from utils import group_small_categories_as_others
# --- Utility Functions ---

def check_auth(admin_only=False):
    """Check authentication and return user_id if successful."""
    # Check if user is logged in
    if not st.session_state.get("logged_in", False):
        st.warning("Por favor, faça login para acessar esta página.")
        if st.button("Ir para Login"):
            st.switch_page("app.py")
        st.stop()
    
    # Check admin access if required
    if admin_only and not st.session_state.get("user_info", {}).get("is_admin", False):
        st.error("Acesso negado. Esta página é restrita a administradores.")
        st.stop()
    
    # Check if user info exists
    if "user_info" not in st.session_state or not st.session_state.user_info.get("user_id"):
        st.error("Informações do usuário não encontradas. Por favor, faça login novamente.")
        if st.button("Ir para Login"):
            st.switch_page("app.py")
        st.stop()
    
    return st.session_state.user_info["user_id"]

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
    st.caption("Selecione as \"provas\" (pastas de imagens) que gostaria de analisar. Os relatórios são gerados automaticamente.")
    
    # Add profile and logout to sidebar
    add_sidebar_profile_and_logout()

def render_marathon_info_cards(marathon_data: Dict[str, Any]):
    """
    Renders cards for a marathon using data directly from database.
    Optimized to work with get_individual_marathon_metrics output.
    
    Args:
        marathon_data: Dictionary containing marathon metrics from database
    """
    if not marathon_data:
        st.info("📋 Nenhum dado disponível para exibição.")
        return
    
    # Extract basic info
    event_date = marathon_data.get('event_date', 'Data não informada')
    location = marathon_data.get('location', 'Local não informado')
    
    # Extract metrics
    total_participants = marathon_data.get('total_participants', 0)
    leader_brand = marathon_data.get('leader_brand', {})
            
    st.subheader("Dados Gerais")
    
    st.caption(f"📅 Data: {event_date}")
    st.caption(f"📍 Local: {location}")
    st.caption(f"👥 Participantes análisados: {total_participants}")
    st.caption(f"🏆 Marca Líder: {leader_brand.get('name', 'N/A')}")
            
    
def render_brand_distribution_chart(marathon_data: Dict[str, Any], highlight=None):
    """
    Renders a horizontal bar chart showing brand distribution with percentages.
    Follows the visual design from the user's example.
    
    Args:
        marathon_data: Dictionary containing marathon metrics from database
        highlight: List of brand names to highlight with a different color
    """
    st.subheader("📊 Distribuição de Marcas")
    
    brand_distribution = marathon_data.get('brand_distribution', {})
    
    if not brand_distribution:
        st.info("📋 Nenhuma marca detectada para gerar o gráfico.")
        return
    
    # Convert to DataFrame and calculate percentages
    total = sum(brand_distribution.values())
    chart_data = []
    
    for brand, count in sorted(brand_distribution.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / total * 100) if total > 0 else 0
        chart_data.append({
            'Marca': brand,
            'Percentual': round(percentage, 1),
            'Contagem': count
        })
    
    if not chart_data:
        st.info("📋 Nenhum dado disponível para o gráfico.")
        return
    
    df = pd.DataFrame(chart_data)
    
    # Define colors - highlight first brand (leader) with red/primary color
    def get_bar_color(brand_name, index):
        if highlight and brand_name in highlight:
            return '#e74c3c'  # Orange for highlighted brands
        else:
            return '#3498db'  # Blue for others
    
    # Add color column to dataframe
    df['color'] = [get_bar_color(row['Marca'], i) for i, row in df.iterrows()]
    
    # Create horizontal bar chart
    chart = alt.Chart(df).mark_bar().encode(
        x=alt.X('Percentual:Q', 
                title='Percentual',
                scale=alt.Scale(domain=[0, max(50, df['Percentual'].max() * 1.1)]),
                axis=alt.Axis(format='.0f')),
        y=alt.Y('Marca:N', 
                title='Marca',
                sort='-x',  # Sort by descending percentual
                axis=alt.Axis(labelLimit=200)),
        color=alt.Color('color:N', 
                       scale=None,  # Use the exact colors we specified
                       legend=None),
        tooltip=[
            alt.Tooltip('Marca:N', title='Marca'),
            alt.Tooltip('Percentual:Q', title='Percentual (%)', format='.1f'),
            alt.Tooltip('Contagem:Q', title='Participantes', format=',d')
        ]
    ).properties(
        height=max(400, len(df) * 40),  # Dynamic height based on number of brands
        width='container'
    ).configure_axis(
        labelFontSize=12,
        titleFontSize=14
    ).configure_view(
        strokeWidth=0  # Remove border
    )
    
    st.altair_chart(chart, use_container_width=True)
    
def render_demographic_analysis(gender_data):
    """
    Renders demographic analysis charts optimized for database data.
    
    Args:
        marathon_data: Dictionary containing marathon metrics from database
    """
    color_scheme = {"MASCULINO": "#1f77b4", "FEMININO": "#990785"}

    st.subheader("👥 Presença de marcas por gênero")
    

        
    # Prepare data for visualization
    chart_data = gender_data.copy()
    #create a percentage column that show the percentage of gender by brand
    chart_data['percentage'] = chart_data['count'] / chart_data.groupby('shoe_brand')['count'].transform('sum')

    # Calculate total count for each brand to determine which ones to group as "Outros"
    brand_totals = chart_data.groupby('shoe_brand')['count'].sum()
    total_participants = chart_data['count'].sum()
    
    # Identify brands with less than 2% of total participants
    brands_to_group = brand_totals[brand_totals / total_participants < 0.02].index.tolist()
    
    # Group small brands as "Outros"
    if brands_to_group:
        # Create a copy to avoid modifying the original data
        chart_data_grouped = chart_data.copy()
        
        # Replace small brands with "Outros"
        chart_data_grouped.loc[chart_data_grouped['shoe_brand'].isin(brands_to_group), 'shoe_brand'] = 'Outros'
        
        # Aggregate the "Outros" data
        chart_data_grouped = chart_data_grouped.groupby(['shoe_brand', 'gender'], as_index=False).agg({
            'count': 'sum'
        })
        
        # Recalculate percentages after grouping
        chart_data_grouped['percentage'] = chart_data_grouped['count'] / chart_data_grouped.groupby('shoe_brand')['count'].transform('sum')
        
        chart_data = chart_data_grouped
    st.dataframe(chart_data, use_container_width=True, hide_index=True)

    # Create a normalized stacked bar chart
    chart = alt.Chart(chart_data).mark_bar().encode(
        y=alt.Y('shoe_brand:N', title='Marca', sort='-x'),
        x=alt.X('percentage:Q', 
                title='Distribuição por Gênero (%)', 
                axis=alt.Axis(format='%'),
                stack='normalize'),
        color=alt.Color('gender:N', 
                       title='Gênero',
                       scale=alt.Scale(
                           domain=color_scheme.keys(),
                           range=color_scheme.values())),
        tooltip=[
            alt.Tooltip('shoe_brand:N', title='Marca'),
            alt.Tooltip('gender:N', title='Gênero'),
            alt.Tooltip('percentage:Q', title='Percentual', format='.1%')
        ]
    ).properties(
        height=max(400, len(chart_data['shoe_brand'].unique()) * 30),
        title='Distribuição de Gênero por Marca'
    ).configure_view(
        strokeWidth=0
    ).configure_axisY(
        labelLimit=200
    )
    
    st.altair_chart(chart, use_container_width=True)

def render_category_distribution_analysis(category_data, highlight=None):
    """
    Renders category distribution analysis charts optimized for database data.
    
    Args:
        category_data: Dictionary containing category distribution metrics from database
    """
    st.subheader("📊 Distribuição de Categorias")
    
    if category_data.empty:
        st.info("📋 Nenhuma categoria detectada para gerar o gráfico.")
        return
    
    # Prepare data for visualization
    chart_data = category_data.copy()
    #cria coluna de percentual dentro do run category usando groupby
    chart_data['percentage'] = chart_data['count'] / chart_data.groupby('run_category')['count'].transform('sum')

 
    chart_data = group_small_categories_as_others(chart_data, 'run_category')
    

    if highlight:
        # Add a color column based on highlight condition
        chart_data['color'] = chart_data['shoe_brand'].apply(
            lambda x: '#e74c3c' if x in highlight else '#3498db'
        )
    # Create a horizontal bar chart
    
    chart = alt.Chart(chart_data).mark_bar().encode(
        x=alt.X('percentage:Q', title='Percentual', axis=alt.Axis(format='%')),
        y=alt.Y('shoe_brand:N', title='Marca', sort='-x'),
        color=alt.Color('color:N', 
            scale=None,  # Use the exact colors we specified
            legend=None),
        tooltip=[
            alt.Tooltip('shoe_brand:N', title='Marca'),
            alt.Tooltip('percentage:Q', title='Percentual', format='.1%'),
        ],
        facet=alt.Facet('run_category:N', title='Categoria')
    ).resolve_scale(
        x='independent',  # Independent x-axis for each category
        y='independent'
    ).properties(
        height=max(400, len(chart_data) * 30),
        title='Distribuição de Categorias'
    ).configure_view(
        strokeWidth=0
    )
    
    st.altair_chart(chart, use_container_width=True)