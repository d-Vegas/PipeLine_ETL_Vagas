import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

# Lista para armazenar os dados das vagas
vagas = []

sites = [
    {
        'nome': 'Engenheiro de Dados',
        'url_base': 'https://www.vagas.com.br/vagas-de-Engenheiro-de-dados?pagina='
    },
    {
        'nome': 'Analista de Dados',
        'url_base': 'https://www.vagas.com.br/vagas-de-analise-de-dados?pagina='
    },
    {
        'nome': 'Cientista de Dados',
        'url_base': 'https://www.vagas.com.br/vagas-de-cientista-de-dados?pagina='
    }
]

# Loop pelos sites
for site in sites:
    print(f"Coletando dados de {site['nome']}...")
    url_base = site['url_base']
    # Loop pelas páginas de resultados (pegando 10 páginas)
    for pagina in range(1, 11):
        url = url_base + str(pagina)
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        soup = BeautifulSoup(response.content, 'html.parser')

        # Encontrar os elementos que contêm as vagas
        for vaga in soup.find_all('li', class_='vaga'):
            titulo_element = vaga.find('a', class_='link-detalhes-vaga')
            if titulo_element:
                titulo = titulo_element.get_text(strip=True)
                link_vaga = 'https://www.vagas.com.br' + titulo_element['href']
            else:
                titulo = 'Título não encontrado'
                link_vaga = None

            descricao_element = vaga.find('div', class_='detalhes')
            if descricao_element:
                descricao = descricao_element.get_text(strip=True)
            else:
                descricao = 'Descrição não encontrada'

            # Extrair o tipo da vaga (júnior, pleno, sênior, estágio)
            tipo_vaga_element = vaga.find('div', class_='nivelVaga')
            if tipo_vaga_element:
                tipo_vaga = tipo_vaga_element.get_text(strip=True)
            else:
                tipo_vaga = 'Não informado'

            # (Opcional) Extrair as habilidades necessárias acessando a página da vaga
            habilidades = 'Não informado'
            if link_vaga:
                response_vaga = requests.get(link_vaga, headers={'User-Agent': 'Mozilla/5.0'})
                soup_vaga = BeautifulSoup(response_vaga.content, 'html.parser')
                habilidades_element = soup_vaga.find('div', class_='conteudo')
                if habilidades_element:
                    habilidades = habilidades_element.get_text(separator=' ', strip=True)

            # Adicionar os dados à lista
            vagas.append({
                'Título': titulo,
                'Descrição': descricao,
                'Tipo da Vaga': tipo_vaga,
                'Habilidades Necessárias': habilidades,
                'Site': site['nome']
            })

        # Aguardar alguns segundos para não sobrecarregar o servidor
        time.sleep(2)

# Criar um DataFrame com os dados coletados
df = pd.DataFrame(vagas)

# Exportar os dados para um arquivo CSVaaaa
df.to_csv('vagas_dados.csv', index=False)

print("Coleta concluída. Dados salvos em 'vagas_dados.csv'.")
