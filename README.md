# Shoes AI

Aplicação web desenvolvida em Streamlit para análise de dados de maratonas a partir de imagens.

## Visão geral

A plataforma permite importar dados de eventos (maratonas) e gerar relatórios automáticos sobre marcas de tênis detectadas, distribuição por gênero e raça, além de comparativos entre provas.

O repositório contém:

- `app.py` – página de login e ponto de entrada do Streamlit.
- Diretório `pages` – páginas de relatório, sobre a plataforma, importador de dados e perfil.
- `database_abstraction.py` – camada de abstração de banco de dados utilizando SQLAlchemy, compatível com SQLite, PostgreSQL e MySQL.
- `data_processing.py` – processamento de métricas a partir dos dados armazenados.
- `manage_db.py` – utilitário de linha de comando para gerenciar usuários e provas.

## Requisitos

- Python 3.10+.
- Dependências listadas em `requirements.txt`.

## Instalação

1. Clone este repositório.
2. Crie um ambiente virtual e instale as dependências:

```bash
pip install -r requirements.txt
```

3. Copie `.env.example` para `.env` e ajuste as variáveis de banco de dados se necessário.
4. Gere um usuário inicial:

```bash
python manage_db.py user add --email usuario@exemplo.com --password senha123
```

5. Inicie a aplicação:

```bash
streamlit run app.py
```

## Uso

Após fazer login, escolha as maratonas que deseja analisar na página **Relatórios**. Também é possível importar novos dados pela página **Importador de Dados**. A seção **Sobre** descreve o propósito da plataforma e a página **Perfil** exibe informações do usuário conectado.

---

Este projeto serve como demonstração interna da Shoes.AI para explorar técnicas de visão computacional aplicadas ao esporte.
