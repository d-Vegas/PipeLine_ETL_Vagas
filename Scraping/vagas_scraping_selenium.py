# %%
# In[0.0]: Importação de Bibliotecas
import requests
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import csv
import pandas as pd
from tqdm import tqdm


# %%
# In[0.1]: Instalação e Configuração do Driver do Navegador Automático

# Importação de Bibliotecas referentes ao Google Chrome
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import os

# Setup Google Chrome
chrome_driver_dir = ChromeDriverManager().install()
chrome_driver_path = os.path.join(os.path.dirname(chrome_driver_dir), 'chromedriver')

#Verificação manual do conteúdo do diretório do driver e ajuste do caminho
"""
Essa parte se fez necessária após a atualização da versão do Google Chrome "127.0.XXXX.XX".
Passou-se a ter um novo arquivo dentro do diretório de instalação do driver 'THIRD_PARTY_NOTICES.chromedriver'
além dos arquivos 'chromedriver.exe' e 'LICENSE.chromedriver'.
Para evitar problemas de identificação do executável do driver correto, é necessário fazer uma verificação do nome
do executável em questão e também atribuí-lo corretamente.
"""
for root, dirs, files in os.walk(os.path.dirname(chrome_driver_dir)):
    for file in files:
        if file == 'chromedriver' or file == 'chromedriver.exe':
            chrome_driver_path = os.path.join(root, file)
            break

# Configuração do serviço para o ChromeDriver
service = Service(chrome_driver_path)


# %%
# In[1.0]: Setup Inicial do Script

# Criação de uma tabela vazia para armazenar os dados a serem extraídos
tabela_de_vagas = {
    "Título":[],
    "Empresa":[],
    "ID":[],
    "Nível_Experiência":[],
    "Data_Publicação":[],
    "Faixa_Salarial":[],
    "Localização":[],
    "Modelo_Contratual":[],
    "Benefícios":[],
    "Descrição_do_Cargo":[]
}                                                                              

# Objeto ids para ser atribuído manualmente os id de cada vaga por iterações à tabela
ids = 1

# Configuração de Sites com Dados de Vagas
sites_url_vagas_com_br = [
    {
        'nome': 'Engenheiro de Dados',
        'url_base': 'https://www.vagas.com.br/vagas-de-engenheiro-de-Dados?'
    },
    {
        'nome': 'Analista de Dados',
        'url_base': 'https://www.vagas.com.br/vagas-de-analista-de-Dados?'
    },
    {
        'nome': 'Cientista de Dados',
        'url_base': 'https://www.vagas.com.br/vagas-de-cientista-de-Dados?'
    }
]

# Criação de um navegador para acessar os sites a serem realizado o Web Scraping
navegador = webdriver.Chrome(service=service)


# %%
# In[1.1]: Web Scraping Para o Site 'www.vagas.com.br' com Navegador Automatizado por Selenium

# Loop inicial para iterar sobre todos os sites de 'www.vagas.com.br'
for site in sites_url_vagas_com_br:
    nome = site['nome']
    url_base = site['url_base']
    site_name = "www.vagas.com.br"
    
    # Mensagem inicial de extração iniciada
    print(f"Iniciando a extração dos dados de Vagas para {nome} do site {site_name}")
    
    # Insere os links no navegador automatizado
    navegador.get(url_base)
    time.sleep(5)
    
    # Fechamento de anúncios e permissões dentro do site
    # botão de aceite de armazenamento de cookies
    try:
        botao_user_agreements = WebDriverWait(navegador, 2).until(
            EC.element_to_be_clickable((By.XPATH, "//span[@class='oPrivallyApp-AcceptLinkA']"))
        )
        navegador.execute_script("arguments[0].click();", botao_user_agreements)
        time.sleep(1)
    except:
        time.sleep(1)
        pass
    # botão de anúncio pop-up
    try:
        iframe_anuncio_1 = WebDriverWait(navegador, 2).until(
            EC.presence_of_element_located((By.XPATH, "//iframe[@data-test-id='interactive-frame']"))
        )
        navegador.switch_to.frame(iframe_anuncio_1)
        botao_anuncio_1 = WebDriverWait(navegador, 2).until(
            EC.element_to_be_clickable((By.XPATH, "//div[@id='interactive-close-button']"))
        )
        navegador.execute_script("arguments[0].click();", botao_anuncio_1)
        time.sleep(1)
        navegador.switch_to.default_content()
    except Exception as e:
        time.sleep(1)
        pass
    # botão de anúncio inferior da página
    try:
        botao_anuncio_2 = WebDriverWait(navegador, 2).until(
            EC.presence_of_element_located((By.XPATH, "//span[@class='r89-sticky-top-close-button']"))
        )
        navegador.execute_script("arguments[0].click();", botao_anuncio_2)
        time.sleep(1)
    except:
        time.sleep(1)
        pass
    
    # Abertura de mais vagas dentro da página
    while True:
        try:
            botao_mais_vagas = WebDriverWait(navegador, 3).until(
                EC.element_to_be_clickable((By.XPATH, "//a[@class='btMaisVagas btn']"))
            )
            navegador.execute_script("arguments[0].scrollIntoView(true);", botao_mais_vagas)
            navegador.execute_script("arguments[0].click();", botao_mais_vagas)
        except:
            break

    # Encontra o número total de vagas na página
    try:
        container_vagas = WebDriverWait(navegador, 4).until(
            EC.presence_of_element_located((By.XPATH, "//div[@id='todasVagas']/ul"))
        )
        vagas = container_vagas.find_elements(By.XPATH, "./li[contains(@class, 'vaga')]")
        total_vagas_extrair = len(vagas)
        print(f"Número de vagas para {nome} encontradas: {total_vagas_extrair}")
    except Exception as e:
        continue

    # Segundo Loop para abrir vaga por vaga no site e puxar dados de cada vaga
    with tqdm(total=total_vagas_extrair, desc=f'Extraindo vagas para {nome}', unit='vaga') as pbar:
        for i, vaga_elemento in enumerate(vagas, start=1):
            # Busca os botões de vaga por vaga e clica neles
            try:
                botao_vaga = vaga_elemento.find_element(By.XPATH, "./header/div[2]/h2/a")
                navegador.execute_script("arguments[0].scrollIntoView(true);", botao_vaga)
                navegador.execute_script("arguments[0].click();", botao_vaga)
                time.sleep(5)

                # Busca e atribui o Título da Vaga à tabela
                titulo = WebDriverWait(navegador, 1).until(
                    EC.visibility_of_element_located((By.XPATH, "//h1[@class='job-shortdescription__title']"))
                ).text
                tabela_de_vagas["Título"].append(titulo)

                # Busca e atribui o Nome da Empresa da Vaga à tabela
                nome_empresa = WebDriverWait(navegador, 1).until(
                    EC.visibility_of_element_located((By.XPATH, "//h2[@class='job-shortdescription__company']"))
                ).text
                tabela_de_vagas["Empresa"].append(nome_empresa)

                # Atribuição Manual do ID da Vaga à tabela
                vaga_id = ids
                tabela_de_vagas["ID"].append(vaga_id)
                ids += 1

                # Busca e atribui o Nível de Experiência da Vaga à tabela
                nivel_experiencia = WebDriverWait(navegador, 1).until(
                    EC.visibility_of_element_located((By.XPATH, "//span[@class='job-hierarchylist__item job-hierarchylist__item--level']"))
                ).text
                tabela_de_vagas["Nível_Experiência"].append(nivel_experiencia)

                # Busca e atribui a Data de Publicação da Vaga à tabela
                data_publicacao = WebDriverWait(navegador, 1).until(
                    EC.visibility_of_element_located((By.XPATH, '/html/body/div/section[1]/div/div[1]/ul/li[1]'))
                ).text
                tabela_de_vagas["Data_Publicação"].append(data_publicacao)
            
                # Busca e atribui a Faixa Salarial da Vaga à tabela
                faixa_salarial = WebDriverWait(navegador, 1).until(
                    EC.visibility_of_element_located((By.XPATH, '/html/body/div/section[2]/main/article/header/div/ul/li[1]/div/span[2]'))
                ).text
                tabela_de_vagas["Faixa_Salarial"].append(faixa_salarial)

                # Busca e atribui a Localização da Vaga à tabela
                localizacao = WebDriverWait(navegador, 1).until(
                    EC.visibility_of_element_located((By.XPATH, "//span[@class='info-localizacao']"))
                ).text
                tabela_de_vagas["Localização"].append(localizacao)

                # Busca e atribui o Modelo Contratual da Vaga à tabela
                modelo_contratual = WebDriverWait(navegador, 1).until(
                    EC.visibility_of_element_located((By.XPATH, "//span[@class='info-modelo-contratual']"))
                ).text
                tabela_de_vagas["Modelo_Contratual"].append(modelo_contratual)

                # Busca e atribui os Benefícios da Vaga à tabela
                beneficios = []
                j = 1
                while True:
                    try:
                        beneficio = WebDriverWait(navegador, 2).until(
                            EC.visibility_of_element_located((By.XPATH, f'/html/body/div/section[2]/main/article/div[2]/ul/li[{j}]/span'))
                        ).text
                        beneficios.append(beneficio)
                        j += 1
                    except:
                        break
                tabela_de_vagas["Benefícios"].append(beneficios)
            
                # Busca e atribui a Descrição da Vaga à tabela
                descricao_cargos = []
                j = 1
                while True:
                    try:
                        descricao_cargo = WebDriverWait(navegador, 2).until(
                            EC.visibility_of_element_located((By.XPATH, f'/html/body/div/section[2]/main/article/div[3]/p[{j}]'))
                        ).text
                        descricao_cargos.append(descricao_cargo)
                        j += 1
                    except:
                        break
                tabela_de_vagas["Descrição_do_Cargo"].append(descricao_cargos)
    
                # Atualiza a barra de progresso
                pbar.update(1)

                # Retorna o navegador para a página anterior e passa a iteração até a próxima vaga
                navegador.back()
                time.sleep(3)

                # Verifica se é necessário apertar o botão de mais vagas dentro do site novamente
                while True:
                    try:
                        botao_mais_vagas = WebDriverWait(navegador, 3).until(
                            EC.element_to_be_clickable((By.XPATH, "//a[@class='btMaisVagas btn']"))
                        )
                        navegador.execute_script("arguments[0].scrollIntoView(true);", botao_mais_vagas)
                        navegador.execute_script("arguments[0].click();", botao_mais_vagas)
                    except:
                        break


            # Mensagem de erro e interrupção do loop em caso de falha
            except Exception as e:
                print(f"Erro na iteração {i}: {e} - Site: {site_name}")
                break

        # Mensagem de conclusão de vagas extraídas
        print(f"Dados de Vagas para {nome} extraídos. Total de Vagas Extraídas: {i}")

# Mensagem de confirmação de todas Vagas Extraídas com sucesso
print(f"Web Scraping das vagas do site {site_name} finalizado.")


# %%
# In[1.2]: Web Scraping Para o Site 'www.empregare.com' com Navegador Automatizado por Selenium


# %%
# In[1.3]: Web Scraping Para o Site 'www.catho.com.br' com Navegador Automatizado por Selenium


# %%
# In[2.0]: Salvando Dados Obtidos em um Banco de Dados com Formato .CSV

# Busca e atribuí o diretório do Project para salvar os arquivos
diretorio_projeto = os.getcwd()
print(f"Salvando os Dados extraídos em formato .csv no diretório: {diretorio_projeto}")

# Criação de um DataFrame do pandas a partir da tabela de vagas com os dados extraídos
df_tabela_de_vagas = pd.DataFrame(tabela_de_vagas)
df_tabela_de_vagas['Benefícios'] = df_tabela_de_vagas['Benefícios'].apply(lambda x: ' + '.join(x))
df_tabela_de_vagas['Descrição_do_Cargo'] = df_tabela_de_vagas['Descrição_do_Cargo'].apply(lambda x: ' '.join(x))

# Criação de um arquivo .csv no diretório do Project que recebe as informações da tabela de vagas
csv_name = "vagas_cargo_de_dados.csv"
df_tabela_de_vagas.to_csv('vagas_scrap_selenium.csv', index=False, encoding='utf-8', sep=';')
print(f"Dados de Vagas Extraídos salvos em {csv_name} dentro do diretório: {diretorio_projeto}")


# %%
