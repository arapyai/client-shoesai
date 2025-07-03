#!/usr/bin/env python3
"""
Script de teste para verificar se os dados estão sendo salvos corretamente no banco.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database_abstraction import db
import json

def test_database_data():
    """Testa se os dados estão sendo salvos e recuperados corretamente."""
    
    print("🔍 Verificando dados no banco de dados...\n")
    
    # 1. Verificar provas existentes
    print("1. Provas existentes:")
    marathons = db.get_marathon_list_from_db()
    if not marathons:
        print("   ❌ Nenhuma prova encontrada no banco!")
        return
    
    for marathon in marathons:
        print(f"   📊 ID: {marathon['id']} | Nome: {marathon['name']}")
    
    print(f"\n   Total de provas: {len(marathons)}\n")
    
    # 2. Para cada prova, verificar dados dos corredores
    for marathon in marathons[:3]:  # Testar apenas as 3 primeiras
        marathon_id = marathon['id']
        marathon_name = marathon['name']
        
        print(f"2. Dados da prova '{marathon_name}' (ID: {marathon_id}):")
        
        # Verificar corredores
        runners = db.get_marathon_runners(marathon_id)
        print(f"   👥 Corredores: {len(runners)}")
        
        if runners:
            # Mostrar alguns exemplos
            print("   📋 Primeiros 3 corredores:")
            for i, runner in enumerate(runners[:3]):
                print(f"      {i+1}. Peito: {runner['bib']} | Marca: {runner['shoe_brand']} | Gênero: {runner['gender']}")
        
        # 3. Verificar estatísticas pré-computadas
        print(f"\n3. Estatísticas para '{marathon_name}':")
        
        # Verificar descrição da prova (onde ficam as estatísticas)
        with db.get_connection() as conn:
            from sqlalchemy import select
            stmt = select(db.marathons.c.description).where(
                db.marathons.c.marathon_id == marathon_id
            )
            result = conn.execute(stmt).fetchone()
            
            if result and result.description:
                if "--- ESTATÍSTICAS AUTOMÁTICAS ---" in result.description:
                    print("   ✅ Estatísticas encontradas na descrição!")
                    
                    # Extrair e mostrar estatísticas
                    try:
                        stats_start = result.description.find("--- ESTATÍSTICAS AUTOMÁTICAS ---")
                        stats_json = result.description[stats_start + len("--- ESTATÍSTICAS AUTOMÁTICAS ---"):].strip()
                        stored_stats = json.loads(stats_json)
                        
                        print(f"      📊 Total participantes: {stored_stats.get('total_participants', 'N/A')}")
                        print(f"      🏷️  Total marcas: {stored_stats.get('total_brands', 'N/A')}")
                        print(f"      🥇 Marca líder: {stored_stats.get('leader_brand', {}).get('name', 'N/A')}")
                        print(f"      🎯 Confiança média: {stored_stats.get('avg_confidence', 'N/A')}")
                        
                    except json.JSONDecodeError as e:
                        print(f"   ❌ Erro ao decodificar estatísticas: {e}")
                else:
                    print("   ⚠️  Estatísticas não encontradas na descrição!")
            else:
                print("   ❌ Descrição vazia ou não encontrada!")
        
        # 4. Testar função get_precomputed_marathon_metrics
        print(f"\n4. Testando get_precomputed_marathon_metrics para '{marathon_name}':")
        metrics = db.get_precomputed_marathon_metrics([marathon_id])
        
        if metrics:
            print("   ✅ Métricas retornadas com sucesso!")
            print(f"      📊 Total shoes detected: {metrics.get('total_shoes_detected', 'N/A')}")
            print(f"      🏷️  Unique brands count: {metrics.get('unique_brands_count', 'N/A')}")
            print(f"      🥇 Leader brand name: {metrics.get('leader_brand_name', 'N/A')}")
        else:
            print("   ❌ Nenhuma métrica retornada!")
        
        # 5. Testar função get_individual_marathon_metrics
        print(f"\n5. Testando get_individual_marathon_metrics para '{marathon_name}':")
        individual_metrics = db.get_individual_marathon_metrics(marathon_id)
        
        if individual_metrics:
            print("   ✅ Métricas individuais retornadas com sucesso!")
            print(f"      📊 Total participantes: {individual_metrics.get('total_participants', 'N/A')}")
            print(f"      🏷️  Total marcas: {individual_metrics.get('total_brands', 'N/A')}")
            print(f"      🥇 Marca líder: {individual_metrics.get('leader_brand', {}).get('name', 'N/A')}")
        else:
            print("   ❌ Nenhuma métrica individual retornada!")
        
        print("\n" + "="*60 + "\n")


if __name__ == "__main__":
    test_database_data()
