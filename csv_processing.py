# csv_processing.py
"""
Módulo para processamento de dados CSV das provas.
Nova estrutura simplificada que processa diretamente os dados do CSV
e gera estatísticas no backend sem armazenar no banco.
"""

import pandas as pd

def load_and_validate_csv(csv_file) -> pd.DataFrame:
    """
    Carrega e valida o arquivo CSV da prova.
    
    Estrutura esperada:
    - bib: número do peito
    - position: posição na categoria (? para não posicionado)
    - gender: MASCULINO/FEMININO
    - run_category: categoria da prova (5K, 10K, 21K, 42K, etc.)
    - shoe_brand: marca do tênis
    - confidence: nível de confiança da detecção
    """
    try:
        # Lê o CSV
        df = pd.read_csv(csv_file)
        
        # Valida colunas obrigatórias
        required_columns = ['bib', 'position', 'gender', 'run_category', 'shoe_brand', 'confidence']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"Colunas obrigatórias ausentes: {missing_columns}")
        
        # Limpeza e normalização dos dados
        df['bib'] = df['bib'].astype(str).str.strip()
        df['position'] = df['position'].astype(str).str.strip()
        df['gender'] = df['gender'].astype(str).str.strip().str.upper()
        df['run_category'] = df['run_category'].astype(str).str.strip()
        df['shoe_brand'] = df['shoe_brand'].astype(str).str.strip()
        df['confidence'] = pd.to_numeric(df['confidence'], errors='coerce')
        
        # Remove linhas com dados críticos faltando
        initial_rows = len(df)
        df = df.dropna(subset=['bib', 'gender', 'run_category', 'shoe_brand'])
        final_rows = len(df)
        
        if initial_rows != final_rows:
            print(f"Removidas {initial_rows - final_rows} linhas com dados incompletos")
        
        return df
        
    except Exception as e:
        raise Exception(f"Erro ao processar CSV: {str(e)}")


def generate_statistics(df: pd.DataFrame, marathon_name: str) -> dict:
    """
    Gera estatísticas completas a partir dos dados do DataFrame.
    """
    try:
        # Estatísticas básicas
        total_participants = len(df)
        
        # Análise por marca
        brand_counts = df['shoe_brand'].value_counts()
        top_brands = brand_counts.to_dict()
        total_brands = len(brand_counts)
        
        # Marca líder
        leader_brand = {
            'name': brand_counts.index[0] if len(brand_counts) > 0 else 'N/A',
            'count': brand_counts.iloc[0] if len(brand_counts) > 0 else 0,
            'percentage': round((brand_counts.iloc[0] / total_participants) * 100, 2) if len(brand_counts) > 0 else 0
        }
        
        # Análise por gênero
        gender_counts = df['gender'].value_counts().to_dict()
        
        # Análise por categoria
        category_counts = df['run_category'].value_counts().to_dict()
        
        # Estatísticas de confiança
        avg_confidence = df['confidence'].mean() if 'confidence' in df.columns else 0
        min_confidence = df['confidence'].min() if 'confidence' in df.columns else 0
        max_confidence = df['confidence'].max() if 'confidence' in df.columns else 0
        
        # Análise de posições válidas
        valid_positions = df[df['position'] != '?']['position']
        positioned_count = len(valid_positions)
        unpositioned_count = total_participants - positioned_count
        
        statistics = {
            'marathon_name': marathon_name,
            'total_participants': total_participants,
            'total_brands': total_brands,
            'top_brands': top_brands,
            'leader_brand': leader_brand,
            'gender_distribution': gender_counts,
            'category_distribution': category_counts,
            'avg_confidence': round(avg_confidence, 2),
            'min_confidence': round(min_confidence, 2),
            'max_confidence': round(max_confidence, 2),
            'positioned_participants': positioned_count,
            'unpositioned_participants': unpositioned_count,
            'positioning_rate': round((positioned_count / total_participants) * 100, 2) if total_participants > 0 else 0
        }
        
        return statistics
        
    except Exception as e:
        raise Exception(f"Erro ao gerar estatísticas: {str(e)}")


def create_summary_dataframe(statistics: dict) -> pd.DataFrame:
    """
    Cria um DataFrame resumido a partir das estatísticas.
    """
    try:
        summary_data = []
        
        # Informações gerais
        summary_data.append({'Métrica': 'Total de Participantes', 'Valor': statistics['total_participants']})
        summary_data.append({'Métrica': 'Total de Marcas', 'Valor': statistics['total_brands']})
        summary_data.append({'Métrica': 'Marca Líder', 'Valor': statistics['leader_brand']['name']})
        summary_data.append({'Métrica': 'Confiança Média', 'Valor': f"{statistics['avg_confidence']:.2f}"})
        summary_data.append({'Métrica': 'Taxa de Posicionamento', 'Valor': f"{statistics['positioning_rate']:.1f}%"})
        
        return pd.DataFrame(summary_data)
        
    except Exception as e:
        raise Exception(f"Erro ao criar DataFrame resumido: {str(e)}")
