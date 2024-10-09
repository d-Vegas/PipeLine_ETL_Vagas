#%% [0.0]
# Importação de Bibliotecas
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

#%% [0.1]
# Instalação e Configuração do Driver do Navegador Automático

# Chrome -> Chrome Driver   --- É necessário baixar o driver do Chrome para que ele opere em conjunto com o webdriver
from webdriver_manager.chrome import ChromeDriverManager                                # Importa o driver do Google Chrome
from selenium.webdriver.chrome.service import Service                                   # Importa o serviço para utilizar o Driver do Google Chrome
import os

# Setup Google Chrome automatizado
chrome_driver_dir = ChromeDriverManager().install()                                     # Instalação do Chrome Driver
chrome_driver_path = os.path.join(os.path.dirname(chrome_driver_dir), 'chromedriver')   # Atribuição do diretório

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

#%% [1.0]
# Setup Inicial do Script

# Configuração de Sites com Dados de Vagas
tabela_de_vagas = {                                                                     # Atribui um dicionário vazio a um objeto 'tabela_de_vagas'
    "Nome da Vaga":[],
    "Nome da Empresa":[],
    "Hierarquia do Cargo":[],
    "Faixa Salarial":[],
    "Localização":[],
    "Modelo Contratual":[],
    "Benefícios":[],
    "Descrição do Cargo":[],
    "Descrição da Empresa":[]
}                                                                              

sites_url_vagas_com_br = [                                                              # Lista de sites a serem utilizados no Web Scraping
    {                                                                                   # Sites divididos entre url e nome de vaga buscada
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

# Criação de um navegador para acessar os sites a serem realizado o Web Scraping
navegador = webdriver.Chrome(service=service)                                           # Cria o navegador automatizado pelo webdriver

#%% [1.1]
# Web Scraping Para o Site 'www.vagas.com.br' com Navegador Automatizado por Selenium 
#navegador = webdriver.Chrome(service=service)
# Atribuição da lista de sites para o navegador
navegador.get("https://www.vagas.com.br/vagas-de-Engenheiro-de-dados?pagina=")          # Insere o link no navegador automatizado
time.sleep(3)                                                                           # Tempo de espera até a próxima ação do navegador

# Fechamento de anúncios e permissões dentro do site
botao_user_agreements = WebDriverWait(navegador, 1).until(                              # Busca o botão de aceitar com o armazenamento de cookies no dispositivo
    EC.element_to_be_clickable((By.XPATH, "//span[@class='oPrivallyApp-AcceptLinkA']"))
)
navegador.execute_script("arguments[0].click();", botao_user_agreements)                # Clica no botão de aceite de permissões de cookies
time.sleep(0.5)                                                                         # Tempo de espera até a próxima ação do navegador
    
iframe_anuncio = WebDriverWait(navegador, 1).until(                                     # Verificação de iframe do anúncio principal
    EC.presence_of_element_located((By.XPATH, "//iframe[@data-test-id='interactive-frame']"))
)
navegador.switch_to.frame(iframe_anuncio)                                               # Muda o contexto do navegador para o iframe do anúncio principal
botao_anuncio_1 = WebDriverWait(navegador, 3).until(                                    # Busca o botão de fechamento do anúncio principal
    EC.element_to_be_clickable((By.XPATH, "//div[@id='interactive-close-button']"))
)
navegador.execute_script("arguments[0].click();", botao_anuncio_1)                      # Clica no botão de fechamento do anúncio principal
time.sleep(0.5)                                                                         # Tempo de espera até a próxima ação do navegador
navegador.switch_to.default_content()                                                   # Retorna o navegador ao contexto principal
botao_mais_vagas = WebDriverWait(navegador, 1).until(                                   # Busca o botão para aparecer mais vagas
    EC.element_to_be_clickable((By.XPATH, "//a[@class='btMaisVagas btn']"))
)
navegador.execute_script("arguments[0].click();", botao_mais_vagas)                     # Clica no botão de mais vagas
time.sleep(0.5)                                                                         # Tempo de espera até a próxima ação do navegador

# Loop para abrir vagas e puxar dados de cada vaga
i = 1
while True:
    try:
        botao_vagas = WebDriverWait(navegador, 1).until(
            EC.element_to_be_clickable((By.XPATH, f'/html/body/div[4]/div[3]/div/div/div[2]/section/section/div/ul/li[{i}]/header/div[2]/h2/a'))
        )
        botao_vagas.click()
        time.sleep(3)

        nome_vaga = WebDriverWait(navegador, 1).until(
            EC.visibility_of_element_located((By.XPATH, '/html/body/div[4]/section[1]/div/div[1]/div/div[2]/h1'))
        ).text
        tabela_de_vagas["Nome da Vaga"].append(nome_vaga)

        nome_empresa = WebDriverWait(navegador, 1).until(
            EC.visibility_of_element_located((By.XPATH, '/html/body/div[4]/section[1]/div/div[1]/div/div[2]/div/h2'))
        ).text
        tabela_de_vagas["Nome da Empresa"].append(nome_empresa)

        hierarquia_cargo = WebDriverWait(navegador, 1).until(
            EC.visibility_of_element_located((By.XPATH, '/html/body/div[4]/section[1]/div/div[1]/div/div[2]/div/div/span/span'))
        ).text
        tabela_de_vagas["Hierarquia do Cargo"].append(hierarquia_cargo)

        faixa_salarial = WebDriverWait(navegador, 1).until(
            EC.visibility_of_element_located((By.XPATH, '/html/body/div[4]/section[2]/main/article/header/div/ul/li[1]/div/span[2]'))
        ).text
        tabela_de_vagas["Faixa Salarial"].append(faixa_salarial)

        localizacao = WebDriverWait(navegador, 1).until(
            EC.visibility_of_element_located((By.XPATH, '/html/body/div[4]/section[2]/main/article/header/div/ul/li[2]/div/span'))
        ).text
        tabela_de_vagas["Localização"].append(localizacao)

        modelo_contratual = WebDriverWait(navegador, 1).until(
            EC.visibility_of_element_located((By.XPATH, '/html/body/div[4]/section[2]/main/article/header/div/ul/li[3]/div/span'))
        ).text
        tabela_de_vagas["Modelo Contratual"].append(modelo_contratual)

        beneficios = []
        j = 1
        while True:
            try:
                beneficio = WebDriverWait(navegador, 1).until(
                    EC.visibility_of_element_located((By.XPATH, f'/html/body/div[4]/section[2]/main/article/div[2]/ul/li[{j}]/span'))
                ).text
                beneficios.append(beneficio)
                j += 1
            except:
                break
        tabela_de_vagas["Benefícios"].append(beneficios)

        descricao_cargos = []
        j = 1
        while True:
            try:
                descricao_cargo = WebDriverWait(navegador, 1).until(
                    EC.visibility_of_element_located((By.XPATH, f'/html/body/div[4]/section[2]/main/article/div[3]/p[{j}]'))
                ).text
                descricao_cargos.append(descricao_cargo)
                j += 1
            except:
                break
        tabela_de_vagas["Descrição do Cargo"].append(descricao_cargos)

        descricao_empresas = []
        j = 2
        while True:
            try:
                descricao_empresa = WebDriverWait(navegador, 1).until(
                    EC.visibility_of_element_located((By.XPATH, f'/html/body/div[4]/section[2]/main/article/div[4]/p[{j}]'))
                ).text
                descricao_empresas.append(descricao_empresa)
                j += 1
            except:
                break
        tabela_de_vagas["Descrição da Empresa"].append(descricao_empresas)
    
        navegador.back()
        i += 1
    except:
        break


# %%
