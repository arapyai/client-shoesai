import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
from datetime import datetime
from ui_components import page_header_with_logout, check_auth, BAR_HEIGHT
from database_abstraction import db

# --- Page Config ---
st.set_page_config(layout="wide", page_title="Shoes AI - Análises Avançadas")

# --- Authentication Check ---
user_id = check_auth()

# --- Page Header ---
page_header_with_logout(
    "📈 Análises Avançadas", 
    "Visualizações avançadas e insights profundos dos dados de corridas",
    key_suffix="advanced"
)

# --- Fetch Marathon List ---
@st.cache_data
def fetch_marathon_options_cached():
    return db.get_marathon_list_from_db()

marathon_options = fetch_marathon_options_cached()

if not marathon_options:
    st.warning("⚠️ Nenhuma prova encontrada. Importe dados primeiro.")
    if st.button("📥 Ir para Importação"):
        st.switch_page("pages/3_📥_Importador_de_Dados.py")
    st.stop()

# --- Marathon Selection ---
marathon_names = [m['name'] for m in marathon_options]
marathon_id_map = {m['name']: m['id'] for m in marathon_options}

selected_marathons = st.multiselect(
    "Selecione provas para análise:",
    options=marathon_names,
    default=[marathon_names[0]] if marathon_names else [],
    key="advanced_marathon_selector"
)

if not selected_marathons:
    st.info("👆 Selecione ao menos uma prova para visualizar as análises.")
    st.stop()

selected_ids = [marathon_id_map[name] for name in selected_marathons]

# --- Load Data ---
@st.cache_data
def load_all_data(marathon_ids):
    """Load data for selected marathons."""
    all_runners = []
    marathon_info = {}
    
    for mid in marathon_ids:
        # Get marathon metadata
        marathon_data = db.get_individual_marathon_metrics(mid)
        if marathon_data:
            marathon_info[mid] = marathon_data
            
            # Get runners data
            runners = db.get_marathon_runners(mid)
            for runner in runners:
                runner['marathon_name'] = marathon_data.get('marathon_name')
                runner['event_date'] = marathon_data.get('event_date')
                all_runners.append(runner)
    
    return pd.DataFrame(all_runners), marathon_info

with st.spinner("🔄 Carregando dados..."):
    runners_df, marathon_info = load_all_data(selected_ids)

if runners_df.empty:
    st.error("❌ Nenhum dado encontrado para as provas selecionadas.")
    st.stop()

# --- Tab Layout ---
tab1, tab2, tab3 = st.tabs(["⏰ Análise Temporal", "🔥 Matriz de Correlação", "🎯 Análise de Diversidade"])

# =============================================================================
# TAB 1: ANÁLISE TEMPORAL
# =============================================================================
with tab1:
    st.header("⏰ Análise Temporal de Marcas")
    
    st.markdown("""
    Esta análise mostra como a popularidade das marcas evolui ao longo das diferentes provas.
    Use estas visualizações para identificar tendências, crescimento ou queda de participação de mercado.
    """)
    
    if len(selected_ids) < 2:
        st.info("💡 Selecione pelo menos 2 provas para visualizar tendências temporais.")
    else:
        # Prepare temporal data
        temporal_data = []
        
        for mid, info in marathon_info.items():
            event_date = info.get('event_date', 'Data desconhecida')
            marathon_name = info.get('marathon_name', 'Prova')
            brand_dist = info.get('brand_distribution', {})
            total = sum(brand_dist.values())
            
            for brand, count in brand_dist.items():
                percentage = (count / total * 100) if total > 0 else 0
                temporal_data.append({
                    'marathon_name': marathon_name,
                    'event_date': event_date,
                    'brand': brand,
                    'count': count,
                    'percentage': percentage
                })
        
        temporal_df = pd.DataFrame(temporal_data)
        
        # Sort by date if possible
        try:
            temporal_df['date_parsed'] = pd.to_datetime(temporal_df['event_date'], errors='coerce')
            temporal_df = temporal_df.sort_values('date_parsed')
        except:
            pass
        
        # Get top brands across all events
        top_brands = temporal_df.groupby('brand')['count'].sum().nlargest(10).index.tolist()
        temporal_df_filtered = temporal_df[temporal_df['brand'].isin(top_brands)]
        
        # Line chart: Evolution of brand share over time
        st.subheader("📈 Evolução da Participação de Marcas")
        
        st.markdown("""
        **Como interpretar:** Este gráfico mostra a trajetória de cada marca ao longo das provas.
        - **Linhas ascendentes** 📈: marca está ganhando popularidade
        - **Linhas descendentes** 📉: marca está perdendo participação
        - **Linhas horizontais** ➡️: marca mantém estabilidade
        - Compare múltiplas marcas para identificar competição direta
        """)
        
        line_chart = alt.Chart(temporal_df_filtered).mark_line(point=True).encode(
            x=alt.X('marathon_name:N', title='Prova', axis=alt.Axis(labelAngle=-45)),
            y=alt.Y('percentage:Q', title='Percentual (%)'),
            color=alt.Color('brand:N', title='Marca', scale=alt.Scale(scheme='category20')),
            tooltip=[
                alt.Tooltip('marathon_name:N', title='Prova'),
                alt.Tooltip('brand:N', title='Marca'),
                alt.Tooltip('percentage:Q', title='Percentual (%)', format='.1f'),
                alt.Tooltip('count:Q', title='Participantes')
            ]
        ).properties(
            height=400,
            title='Evolução Temporal das Marcas'
        ).configure_view(strokeWidth=0)
        
        st.altair_chart(line_chart, use_container_width=True)
        
        # Bar chart: Side-by-side comparison
        st.subheader("📊 Comparação Entre Provas")
        
        st.markdown("""
        **Como interpretar:** Visualize lado a lado a distribuição de marcas em cada prova selecionada.
        - **Barras mais altas**: maior presença da marca naquela prova
        - **Compare cores**: cada cor representa uma prova diferente
        - Identifique quais marcas dominam em eventos específicos
        """)
        
        bar_chart = alt.Chart(temporal_df_filtered).mark_bar().encode(
            x=alt.X('brand:N', title='Marca', axis=alt.Axis(labelAngle=-45)),
            y=alt.Y('percentage:Q', title='Percentual (%)'),
            color=alt.Color('marathon_name:N', title='Prova'),
            xOffset='marathon_name:N',
            tooltip=[
                alt.Tooltip('brand:N', title='Marca'),
                alt.Tooltip('marathon_name:N', title='Prova'),
                alt.Tooltip('percentage:Q', title='Percentual (%)', format='.1f')
            ]
        ).properties(
            height=400,
            title='Comparação de Distribuição de Marcas por Prova'
        ).configure_view(strokeWidth=0)
        
        st.altair_chart(bar_chart, use_container_width=True)
        
        # Growth/decline analysis
        st.subheader("📉 Análise de Crescimento")
        
        st.markdown("""
        **Como interpretar:** Este gráfico mostra a variação percentual de cada marca entre a primeira e última prova selecionada.
        - **Verde (positivo)** 🟢: marca cresceu em participação de mercado
        - **Vermelho (negativo)** 🔴: marca perdeu participação de mercado
        - **Valores maiores**: mudanças mais significativas (atenção especial!)
        - Use para identificar marcas em ascensão ou declínio
        """)
        
        if len(selected_ids) >= 2:
            # Calculate growth between first and last event
            marathons_sorted = temporal_df['marathon_name'].unique()
            first_event = marathons_sorted[0]
            last_event = marathons_sorted[-1]
            
            first_data = temporal_df[temporal_df['marathon_name'] == first_event].set_index('brand')['percentage']
            last_data = temporal_df[temporal_df['marathon_name'] == last_event].set_index('brand')['percentage']
            
            growth = (last_data - first_data).dropna().sort_values(ascending=False)
            
            growth_df = pd.DataFrame({
                'Marca': growth.index,
                'Variação (%)': growth.values
            })
            
            growth_df['Tipo'] = growth_df['Variação (%)'].apply(
                lambda x: 'Crescimento' if x > 0 else 'Queda'
            )
            
            growth_chart = alt.Chart(growth_df.head(15)).mark_bar().encode(
                x=alt.X('Variação (%):Q', title='Variação Percentual'),
                y=alt.Y('Marca:N', title='Marca', sort='-x'),
                color=alt.Color('Tipo:N', 
                               scale=alt.Scale(domain=['Crescimento', 'Queda'], 
                                             range=['#2ecc71', '#e74c3c']),
                               legend=alt.Legend(title='Tendência')),
                tooltip=[
                    alt.Tooltip('Marca:N', title='Marca'),
                    alt.Tooltip('Variação (%):Q', title='Variação (%)', format='.2f')
                ]
            ).properties(
                height=alt.Step(BAR_HEIGHT),
                title=f'Variação de Participação: {first_event} → {last_event}'
            ).configure_view(strokeWidth=0)
            
            st.altair_chart(growth_chart, use_container_width=True)

# =============================================================================
# TAB 2: MATRIZ DE CORRELAÇÃO
# =============================================================================
with tab2:
    st.header("🔥 Matriz de Correlação: Categoria × Marca")
    
    st.markdown("""
    Esta análise revela quais marcas dominam em quais categorias de corrida (5K, 10K, 21K, etc.).
    Descubra nichos de mercado e preferências por tipo de prova.
    """)
    
    # Filter data with valid categories and brands
    filtered_df = runners_df[
        (runners_df['run_category'].notna()) & 
        (runners_df['shoe_brand'].notna())
    ].copy()
    
    if filtered_df.empty:
        st.warning("⚠️ Não há dados suficientes para análise de correlação.")
    else:
        # Create pivot table: brands vs categories
        pivot_data = filtered_df.groupby(['shoe_brand', 'run_category']).size().reset_index(name='count')
        
        # Calculate percentage within each category
        category_totals = pivot_data.groupby('run_category')['count'].transform('sum')
        pivot_data['percentage'] = (pivot_data['count'] / category_totals) * 100
        
        # Filter to top brands
        top_brands = pivot_data.groupby('shoe_brand')['count'].sum().nlargest(15).index
        pivot_filtered = pivot_data[pivot_data['shoe_brand'].isin(top_brands)]
        
        # Heatmap
        st.subheader("🌡️ Mapa de Calor: Marca × Categoria")
        
        st.markdown("""
        **Como interpretar:** O mapa de calor mostra a "temperatura" da presença de cada marca em cada categoria.
        - **Cores mais claras/quentes** 🟡: maior participação da marca naquela categoria
        - **Cores mais escuras/frias** 🟣: menor participação
        - **Padrões verticais**: marcas que dominam várias categorias
        - **Padrões horizontais**: categorias com uma marca dominante
        - Identifique especializações: "Nike domina 10K" ou "Asics é forte em maratonas"
        """)
        
        heatmap = alt.Chart(pivot_filtered).mark_rect().encode(
            x=alt.X('run_category:N', title='Categoria', axis=alt.Axis(labelAngle=-45)),
            y=alt.Y('shoe_brand:N', title='Marca', sort='-x'),
            color=alt.Color('percentage:Q', 
                           title='Participação (%)',
                           scale=alt.Scale(scheme='viridis')),
            tooltip=[
                alt.Tooltip('shoe_brand:N', title='Marca'),
                alt.Tooltip('run_category:N', title='Categoria'),
                alt.Tooltip('count:Q', title='Participantes'),
                alt.Tooltip('percentage:Q', title='Percentual (%)', format='.1f')
            ]
        ).properties(
            height=alt.Step(BAR_HEIGHT),
            title='Distribuição de Marcas por Categoria'
        ).configure_view(strokeWidth=0)
        
        st.altair_chart(heatmap, use_container_width=True)
        
        # Dominant brand per category
        st.subheader("👑 Marca Dominante por Categoria")
        
        st.markdown("""
        **Como interpretar:** Identifica qual marca tem maior presença em cada categoria de corrida.
        - **Barras coloridas**: cada cor representa a marca líder daquela categoria
        - **Comprimento da barra**: indica o percentual de dominância
        - **Domínio forte**: >40% de participação
        - **Domínio moderado**: 25-40% de participação
        - **Mercado fragmentado**: <25% para o líder
        - Use para estratégias de patrocínio e marketing por categoria
        """)
        
        dominant = pivot_data.loc[pivot_data.groupby('run_category')['count'].idxmax()]
        dominant = dominant.sort_values('count', ascending=False)
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            dominant_chart = alt.Chart(dominant).mark_bar().encode(
                x=alt.X('percentage:Q', title='Participação (%)'),
                y=alt.Y('run_category:N', title='Categoria', sort='-x'),
                color=alt.Color('shoe_brand:N', title='Marca', 
                              scale=alt.Scale(scheme='category20')),
                tooltip=[
                    alt.Tooltip('run_category:N', title='Categoria'),
                    alt.Tooltip('shoe_brand:N', title='Marca Dominante'),
                    alt.Tooltip('percentage:Q', title='Participação (%)', format='.1f'),
                    alt.Tooltip('count:Q', title='Participantes')
                ]
            ).properties(
                height=alt.Step(BAR_HEIGHT),
                title='Marca Líder em Cada Categoria'
            ).configure_view(strokeWidth=0)
            
            st.altair_chart(dominant_chart, use_container_width=True)
        
        with col2:
            st.dataframe(
                dominant[['run_category', 'shoe_brand', 'count', 'percentage']].rename(columns={
                    'run_category': 'Categoria',
                    'shoe_brand': 'Marca Líder',
                    'count': 'Participantes',
                    'percentage': 'Participação (%)'
                }).style.format({'Participação (%)': '{:.1f}'}),
                hide_index=True,
                use_container_width=True
            )

# =============================================================================
# TAB 3: ANÁLISE DE DIVERSIDADE
# =============================================================================
with tab3:
    st.header("🎯 Análise de Diversidade de Mercado")
    
    st.markdown("""
    Mede o quão concentrado ou diversificado é o mercado de cada prova.
    Alta concentração = poucas marcas dominam. Alta diversidade = muitas marcas competindo em igualdade.
    """)
    
    # Calculate metrics for each marathon
    diversity_data = []
    
    for mid, info in marathon_info.items():
        marathon_name = info.get('marathon_name', 'Prova')
        brand_dist = info.get('brand_distribution', {})
        total = sum(brand_dist.values())
        
        if total == 0:
            continue
        
        # Calculate Herfindahl-Hirschman Index (HHI)
        market_shares = np.array([count / total for count in brand_dist.values()])
        hhi = np.sum(market_shares ** 2) * 10000  # Scale to 0-10000
        
        # Calculate entropy (diversity measure)
        entropy = -np.sum(market_shares * np.log2(market_shares + 1e-10))
        
        # Top 3 market share
        top3_brands = sorted(brand_dist.items(), key=lambda x: x[1], reverse=True)[:3]
        top3_share = sum([count for _, count in top3_brands]) / total * 100
        
        diversity_data.append({
            'marathon_name': marathon_name,
            'total_brands': len(brand_dist),
            'hhi': hhi,
            'entropy': entropy,
            'top3_share': top3_share,
            'leader_brand': info.get('leader_brand', {}).get('name', 'N/A'),
            'leader_share': info.get('leader_brand', {}).get('percentage', 0)
        })
    
    diversity_df = pd.DataFrame(diversity_data)
    
    # Add color column based on HHI
    def get_hhi_color(hhi):
        if hhi > 2500:
            return 'Alta Concentração'
        elif hhi > 1500:
            return 'Concentração Moderada'
        else:
            return 'Baixa Concentração'
    
    diversity_df['concentration_level'] = diversity_df['hhi'].apply(get_hhi_color)
    
    # Display metrics
    st.subheader("📊 Métricas de Diversidade")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        avg_brands = diversity_df['total_brands'].mean()
        st.metric("Média de Marcas", f"{avg_brands:.1f}")
    
    with col2:
        avg_hhi = diversity_df['hhi'].mean()
        concentration = "Alta" if avg_hhi > 2500 else "Moderada" if avg_hhi > 1500 else "Baixa"
        st.metric("HHI Médio", f"{avg_hhi:.0f}", concentration)
    
    with col3:
        avg_top3 = diversity_df['top3_share'].mean()
        st.metric("Top 3 Market Share", f"{avg_top3:.1f}%")
    
    with col4:
        avg_entropy = diversity_df['entropy'].mean()
        st.metric("Entropia Média", f"{avg_entropy:.2f}")
    
    # Explanation
    with st.expander("ℹ️ O que significam essas métricas?"):
        st.markdown("""
        **HHI (Herfindahl-Hirschman Index):**
        - 0-1500: Mercado não concentrado (alta diversidade)
        - 1500-2500: Mercado moderadamente concentrado
        - 2500-10000: Mercado altamente concentrado (baixa diversidade)
        
        **Entropia:**
        - Valores mais altos indicam maior diversidade
        - Mede a "desordem" ou distribuição uniforme das marcas
        
        **Top 3 Market Share:**
        - Percentual total das 3 marcas mais populares
        - Valores altos indicam dominância de poucas marcas
        """)
    
    # Visualizations
    row1_col1, row1_col2 = st.columns(2)
    
    with row1_col1:
        st.subheader("📈 Índice de Concentração (HHI)")
        
        st.markdown("""
        **Como interpretar o HHI:**
        - 🟢 **Verde (0-1500)**: Mercado saudável e diversificado
        - 🟠 **Laranja (1500-2500)**: Concentração moderada
        - 🔴 **Vermelho (>2500)**: Alta concentração, poucas marcas dominam
        
        *Quanto menor o HHI, maior a competição entre marcas.*
        """)
        
        hhi_chart = alt.Chart(diversity_df).mark_bar().encode(
            x=alt.X('hhi:Q', title='Índice HHI', scale=alt.Scale(domain=[0, 10000])),
            y=alt.Y('marathon_name:N', title='Prova', sort='-x'),
            color=alt.Color('concentration_level:N', 
                           title='Nível de Concentração',
                           scale=alt.Scale(
                               domain=['Baixa Concentração', 'Concentração Moderada', 'Alta Concentração'],
                               range=['#2ecc71', '#f39c12', '#e74c3c']
                           )),
            tooltip=[
                alt.Tooltip('marathon_name:N', title='Prova'),
                alt.Tooltip('hhi:Q', title='HHI', format='.0f'),
                alt.Tooltip('total_brands:Q', title='Total de Marcas'),
                alt.Tooltip('concentration_level:N', title='Nível')
            ]
        ).properties(
            height=alt.Step(BAR_HEIGHT)
        ).configure_view(strokeWidth=0)
        
        st.altair_chart(hhi_chart, use_container_width=True)
    
    with row1_col2:
        st.subheader("🎯 Market Share do Top 3")
        
        st.markdown("""
        **Como interpretar:**
        - **>75%**: As 3 maiores marcas dominam quase todo o mercado
        - **50-75%**: Dominância moderada do top 3
        - **<50%**: Mercado fragmentado, várias marcas competindo
        
        *Ideal para identificar concentração de poder de mercado.*
        """)
        
        top3_chart = alt.Chart(diversity_df).mark_bar().encode(
            x=alt.X('top3_share:Q', title='Top 3 Share (%)', scale=alt.Scale(domain=[0, 100])),
            y=alt.Y('marathon_name:N', title='Prova', sort='-x'),
            color=alt.value('#3498db'),
            tooltip=[
                alt.Tooltip('marathon_name:N', title='Prova'),
                alt.Tooltip('top3_share:Q', title='Top 3 Share (%)', format='.1f'),
                alt.Tooltip('leader_brand:N', title='Marca Líder'),
                alt.Tooltip('leader_share:Q', title='Share do Líder (%)', format='.1f')
            ]
        ).properties(
            height=alt.Step(BAR_HEIGHT)
        ).configure_view(strokeWidth=0)
        
        st.altair_chart(top3_chart, use_container_width=True)
    
    # Detailed table
    st.subheader("📋 Tabela Detalhada de Diversidade")
    
    st.markdown("""
    **Legenda das colunas:**
    - **Total Marcas**: Número de marcas diferentes detectadas
    - **HHI**: Índice de concentração (menor = mais diverso)
    - **Entropia**: Medida de "desordem" do mercado (maior = mais diverso)
    - **Top 3 (%)**: Participação combinada das 3 maiores marcas
    - **Share Líder (%)**: Participação apenas da marca líder
    
    *Cores do gradiente: verde = mais diverso, vermelho = mais concentrado*
    """)
    
    display_df = diversity_df.copy()
    display_df = display_df.rename(columns={
        'marathon_name': 'Prova',
        'total_brands': 'Total Marcas',
        'hhi': 'HHI',
        'entropy': 'Entropia',
        'top3_share': 'Top 3 (%)',
        'leader_brand': 'Marca Líder',
        'leader_share': 'Share Líder (%)'
    })
    
    st.dataframe(
        display_df.style.format({
            'HHI': '{:.0f}',
            'Entropia': '{:.2f}',
            'Top 3 (%)': '{:.1f}',
            'Share Líder (%)': '{:.1f}'
        }).background_gradient(subset=['HHI'], cmap='RdYlGn_r'),
        hide_index=True,
        use_container_width=True
    )
    
    # Market concentration interpretation
    st.subheader("💡 Interpretação e Insights")
    
    st.markdown("""
    **Por que isso importa?**
    - 🎯 **Para organizadores**: Provas diversas atraem público variado
    - 📊 **Para marcas**: Identifique oportunidades em mercados fragmentados
    - 🏃 **Para análise**: Entenda preferências e padrões de consumo
    - 💼 **Para patrocínio**: Mercados concentrados = difícil entrar; Mercados diversos = mais oportunidades
    """)
    
    if len(diversity_df) > 0:
        most_diverse = diversity_df.loc[diversity_df['hhi'].idxmin()]
        most_concentrated = diversity_df.loc[diversity_df['hhi'].idxmax()]
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.success(f"""
            **Prova Mais Diversa:**
            - {most_diverse['marathon_name']}
            - {most_diverse['total_brands']} marcas diferentes
            - HHI: {most_diverse['hhi']:.0f}
            - Distribuição mais equilibrada
            """)
        
        with col2:
            st.warning(f"""
            **Prova Mais Concentrada:**
            - {most_concentrated['marathon_name']}
            - Dominada por: {most_concentrated['leader_brand']} ({most_concentrated['leader_share']:.1f}%)
            - HHI: {most_concentrated['hhi']:.0f}
            - Menor diversidade de marcas
            """)
