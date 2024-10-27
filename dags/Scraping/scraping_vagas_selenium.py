# %%
# In[0.0]: Importação de Bibliotecas
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchWindowException, TimeoutException
import time
import pandas as pd
from tqdm import tqdm
import os
import logging


# %%
# In[0.1]: Instalação e Configuração do Driver do Navegador Automático

# Importação de Bibliotecas referentes ao Google Chrome
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

# Setup Google Chrome
chrome_driver_dir = ChromeDriverManager().install()
chrome_driver_path = os.path.join(os.path.dirname(chrome_driver_dir), 'chromedriver')
chrome_options = Options().add_argument("--start-minimized")

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
chrome_service = Service(chrome_driver_path)


# %%
# In[1.0]: Setup Inicial do Script

# Criação de uma tabela vazia para armazenar os dados a serem extraídos
tabela_de_vagas = {
    "nome_vaga":[],
    "empresa":[],
    "id":[],
    "nivel_experiencia":[],
    "data_publicacao":[],
    "faixa_salario":[],
    "localizacao":[],
    "modelo_contrato":[],
    "beneficios":[],
    "descricao_vaga":[]
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

sites_url_empregare_com = [
    {
        'nome': 'Engenheiro de Dados',
        'url_base': 'https://www.empregare.com/pt-br/vagas?query=Engenheiro%20De%20Dados'
    },
    {
        'nome': 'Analista de Dados',
        'url_base': 'https://www.empregare.com/pt-br/vagas?query=Analista%20De%20Dados'
    },
    {
        'nome': 'Cientista de Dados',
        'url_base': 'https://www.empregare.com/pt-br/vagas?query=Cientista%20De%20Dados'
    }
]


# Suprime mensagens do TensorFlow Lite
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

# Configura o nível de logging para suprimir mensagens indesejadas (somente erros)
logging.basicConfig(level=logging.ERROR)

# Criação de um navegador para acessar os sites a serem realizado o Web Scraping
navegador = webdriver.Chrome(service=chrome_service, options=chrome_options)


# %%
# In[1.1]: Web Scraping Para o Site 'www.vagas.com.br' com Navegador Automatizado por Selenium

site_name = "www.vagas.com.br"
print(f"\nIniciando a extração dos dados de Vagas do site {site_name}.\n")
total_vagas_extraidas_1 = 0

# Loop inicial para iterar sobre todos os sites de 'www.vagas.com.br'
for site in sites_url_vagas_com_br:
    nome = site['nome']
    url_base = site['url_base']
    
    # Insere os links no navegador automatizado
    navegador.get(url_base)
    time.sleep(7)
    
    # Fechamento de anúncios e permissões dentro do site
    # botão de aceite de armazenamento de cookies
    try:
        botao_user_agreements = WebDriverWait(navegador, 2).until(
            EC.element_to_be_clickable((By.XPATH, "//span[@class='oPrivallyApp-AcceptLinkA']"))
        )
        navegador.execute_script("arguments[0].click();", botao_user_agreements)
    except:
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
        navegador.switch_to.default_content()
    except:
        pass
    # botão de anúncio inferior da página
    try:
        botao_anuncio_2 = WebDriverWait(navegador, 2).until(
            EC.presence_of_element_located((By.XPATH, "//span[@class='r89-sticky-top-close-button']"))
        )
        navegador.execute_script("arguments[0].click();", botao_anuncio_2)
    except:
        pass
    
    # Abertura de mais vagas dentro da página
    while True:
        try:
            botao_mais_vagas = WebDriverWait(navegador, 1).until(
                EC.element_to_be_clickable((By.XPATH, "//a[@class='btMaisVagas btn']"))
            )
            navegador.execute_script("arguments[0].scrollIntoView(true);", botao_mais_vagas)
            navegador.execute_script("arguments[0].click();", botao_mais_vagas)
        except:
            break

    # Encontra o número total de vagas na página
    try:
        container_vagas = WebDriverWait(navegador, 1).until(
            EC.presence_of_element_located((By.XPATH, "//div[@id='todasVagas']/ul"))
        )
        vagas = container_vagas.find_elements(By.XPATH, "./li[contains(@class, 'vaga')]")
        total_vagas_extrair = len(vagas)
        print(f"Busca por {nome} - Vagas Encontradas: {total_vagas_extrair}")
    except:
        break

    # Segundo Loop para abrir vaga por vaga no site e puxar dados de cada vaga
    with tqdm(total=total_vagas_extrair, desc='Extraindo vagas', unit='vaga') as pbar:
        for i in range(1, total_vagas_extrair + 1):
            # Busca os botões de vaga por vaga e clica neles
            try:
                botao_vaga = navegador.find_element(By.XPATH, f"/html/body/div/div[3]/div/div/div[2]/section/section/div/ul/li[{i}]/header/div[2]/h2/a")
                navegador.execute_script("arguments[0].scrollIntoView(true);", botao_vaga)
                navegador.execute_script("arguments[0].click();", botao_vaga)
                time.sleep(4.5)

                # Busca e atribui o Titulo da Vaga à tabela
                titulo = WebDriverWait(navegador, 0.1).until(
                    EC.visibility_of_element_located((By.XPATH, "//h1[@class='job-shortdescription__title']"))
                ).text
                tabela_de_vagas["nome_vaga"].append(titulo)

                # Busca e atribui o Nome da Empresa da Vaga à tabela
                nome_empresa = WebDriverWait(navegador, 0.1).until(
                    EC.visibility_of_element_located((By.XPATH, "//h2[@class='job-shortdescription__company']"))
                ).text
                tabela_de_vagas["empresa"].append(nome_empresa)

                # Atribuição Manual do ID da Vaga à tabela
                vaga_id = ids
                tabela_de_vagas["id"].append(vaga_id)
                ids += 1

                # Busca e atribui o Nível de Experiência da Vaga à tabela
                nivel_experiencia = WebDriverWait(navegador, 0.1).until(
                    EC.visibility_of_element_located((By.XPATH, "//span[@class='job-hierarchylist__item job-hierarchylist__item--level']"))
                ).text
                tabela_de_vagas["nivel_experiencia"].append(nivel_experiencia)

                # Busca e atribui a Data de Publicação da Vaga à tabela
                data_publicacao = WebDriverWait(navegador, 0.1).until(
                    EC.visibility_of_element_located((By.XPATH, "//li[@class='job-breadcrumb__item job-breadcrumb__item--published job-breadcrumb__item--nostyle']"))
                ).text
                tabela_de_vagas["data_publicacao"].append(data_publicacao)
            
                # Busca e atribui a Faixa Salarial da Vaga à tabela
                faixa_salarial = WebDriverWait(navegador, 0.1).until(
                    EC.visibility_of_element_located((By.XPATH, "//ul[@class='clearfix']/li[1]/div/span[2]"))
                ).text
                tabela_de_vagas["faixa_salario"].append(faixa_salarial)

                # Busca e atribui a Localizacao da Vaga à tabela
                localizacao = WebDriverWait(navegador, 0.1).until(
                    EC.visibility_of_element_located((By.XPATH, "//span[@class='info-localizacao']"))
                ).text
                tabela_de_vagas["localizacao"].append(localizacao)

                # Busca e atribui o Modelo Contratual da Vaga à tabela
                modelo_contratual = WebDriverWait(navegador, 0.1).until(
                    EC.visibility_of_element_located((By.XPATH, "//span[@class='info-modelo-contratual']"))
                ).text
                tabela_de_vagas["modelo_contrato"].append(modelo_contratual)

                # Busca e atribui os Beneficios da Vaga à tabela
                beneficios = []
                j = 1
                while True:
                    try:
                        beneficio = WebDriverWait(navegador, 0.1).until(
                            EC.visibility_of_element_located((By.XPATH, f'/html/body/div/section[2]/main/article/div[2]/ul/li[{j}]/span'))
                        ).text
                        beneficios.append(beneficio)
                        j += 1
                    except:
                        break
                tabela_de_vagas["beneficios"].append(beneficios)
            
                # Busca e atribui a Descrição da Vaga à tabela
                descricao_cargos = []
                j = 1
                while True:
                    try:
                        descricao_cargo = WebDriverWait(navegador, 0.1).until(
                            EC.visibility_of_element_located((By.XPATH, f'/html/body/div/section[2]/main/article/div[3]/p[{j}]'))
                        ).text
                        descricao_cargos.append(descricao_cargo)
                        j += 1
                    except:
                        break
                tabela_de_vagas["descricao_vaga"].append(descricao_cargos)
    
                # Atualiza a barra de progresso
                pbar.update(1)

                # Retorna o navegador para a página anterior e passa a iteração até a próxima vaga
                navegador.back()
                time.sleep(4.5)

                # Verifica se é necessário apertar o botão de mais vagas dentro do site novamente
                while True:
                    try:
                        botao_mais_vagas = WebDriverWait(navegador, 0.1).until(
                            EC.element_to_be_clickable((By.XPATH, "//a[@class='btMaisVagas btn']"))
                        )
                        navegador.execute_script("arguments[0].scrollIntoView(true);", botao_mais_vagas)
                        navegador.execute_script("arguments[0].click();", botao_mais_vagas)
                    except:
                        break

                total_vagas_extraidas_1 += 1


            # Mensagem de erro e interrupção do loop em caso de falha
            except Exception as e:
                print(f"Erro na iteração {i}: {e} - Site: {site_name}")
                break

        # Mensagem de conclusão de vagas extraídas
        print(f"Busca por {nome} concluído. Total de Vagas Extraídas: {i}\n")

# Mensagem de confirmação de todas Vagas Extraídas com sucesso
print(f"Web Scraping de {site_name} finalizado - Vagas Extraídas: {total_vagas_extraidas_1}.\n")


# %%
# In[1.2]: Web Scraping Para o Site 'www.empregare.com' com Navegador Automatizado por Selenium

site_name = "www.empregare.com"
print(f"\nIniciando a extração de dados de Vagas do site {site_name}.\n")
total_vagas_extraidas_2 = 0

# Loop inicial para iterar sobre todos os sites de 'www.empregare.com'
for site in sites_url_empregare_com:
    nome = site['nome']
    url_base = site['url_base']
    
    # Insere os links no navegador automatizado
    navegador.get(url_base)
    time.sleep(5)

    # Fechamento de anúncios e permissões dentro do site
    # botão de aceite de armazenamento de cookies
    try:
        botao_user_agreements = WebDriverWait(navegador, 2).until(
            EC.element_to_be_clickable((By.XPATH, "//button[@id='onetrust-accept-btn-handler']"))
        )
        navegador.execute_script("arguments[0].click();", botao_user_agreements)
    except:
        pass

    # Rolagem até o final da página para abrir todas vagas
    last_page_height = navegador.execute_script("return document.body.scrollHeight")
    while True:
        try:
            navegador.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
            new_page_height = navegador.execute_script("return document.body.scrollHeight")
            if new_page_height == last_page_height:
                break
            last_page_height = new_page_height
        except:
            pass

    # Encontra o número total de vagas na página
    try:
        container_vagas = WebDriverWait(navegador, 2).until(
            EC.presence_of_element_located((By.XPATH, "//section[@id='listagem-vagas']"))
        )
        vagas = container_vagas.find_elements(By.XPATH, "//a[@class='text-decoration-none']")
        total_vagas_extrair = len(vagas)
        print(f"Busca por {nome} - Vagas Encontradas: {total_vagas_extrair}")
    except:
        break

    # Segundo Loop para abrir vaga por vaga no site e puxar dados de cada vaga
    with tqdm(total=total_vagas_extrair, desc='Extraindo vagas', unit='vaga') as pbar:
        for i in range(1, total_vagas_extrair + 1):
            # Busca os botões de vaga por vaga e clica neles
            try:
                botao_vaga = WebDriverWait(navegador, 0.1).until(
                    EC.presence_of_element_located((By.XPATH, f"/html/body/main/section/div[2]/div[1]/section/a[{i}]/article/div/div/div[2]"))
                )
                navegador.execute_script("arguments[0].scrollIntoView(true);", botao_vaga)
                time.sleep(1)
                navegador.execute_script("arguments[0].click();", botao_vaga)

                # Roda toda a página para abrir todas informações
                last_page_height = navegador.execute_script("return document.body.scrollHeight")
                while True:
                    try:
                        navegador.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        time.sleep(1)
                        new_page_height = navegador.execute_script("return document.body.scrollHeight")
                        if new_page_height == last_page_height:
                            break
                        last_page_height = new_page_height
                    except:
                        continue

                # Busca e atribui o Titulo da Vaga à tabela
                titulo = WebDriverWait(navegador, 0.1).until(
                    EC.visibility_of_element_located((By.XPATH, "//h1[@class='ps-2 ps-md-0 h4 fw-bold m-0 ajuste-titulo-vaga']"))
                ).text
                tabela_de_vagas["nome_vaga"].append(titulo)

                # Busca e atribui o Nome da Empresa da Vaga à tabela
                nome_empresa = WebDriverWait(navegador, 0.1).until(
                    EC.visibility_of_element_located((By.XPATH, "//p[@class='ps-2 ps-md-0 m-0 ajuste-nome-empresa']"))
                ).text
                tabela_de_vagas["empresa"].append(nome_empresa)

                # Atribuição Manual do ID da Vaga à tabela
                vaga_id = ids
                tabela_de_vagas["id"].append(vaga_id)
                ids += 1

                ### Arrumar com if a partir do título da Vaga
                # Busca e atribui o Nível de Experiência da Vaga à tabela
                nivel_experiencia = ""
                tabela_de_vagas["nivel_experiencia"].append(nivel_experiencia)

                # Busca e atribui a Data de Publicação da Vaga à tabela
                data_publicacao = WebDriverWait(navegador, 0.1).until(
                    EC.visibility_of_element_located((By.XPATH, "//p[@class='mb-0']/small/b"))
                ).text
                tabela_de_vagas["data_publicacao"].append(data_publicacao)

                # Busca e atribui a Faixa Salarial da Vaga à tabela
                try:
                    faixa_salarial = WebDriverWait(navegador, 0.1).until(
                        EC.visibility_of_element_located((By.XPATH, '/html/body/main/div/div/div[1]/div[1]/div[1]/div/div[2]/div[3]/ul/li[2]/span/span/span'))
                    ).text
                    tabela_de_vagas["faixa_salario"].append(faixa_salarial)
                except:
                    try:
                        faixa_salarial = "A Combinar"
                        tabela_de_vagas["faixa_salario"].append(faixa_salarial)
                    except:
                        break

                # Busca e atribui a Localizacao da Vaga à tabela
                localizacao = WebDriverWait(navegador, 0.1).until(
                    EC.visibility_of_element_located((By.XPATH, '/html/body/main/div/div/div[1]/div[1]/div[1]/div/ul/li[1]'))
                ).text
                tabela_de_vagas["localizacao"].append(localizacao)

                # Busca e atribui o Modelo Contratual da Vaga à tabela
                try:
                    modelo_contratual = WebDriverWait(navegador, 0.1).until(
                        EC.visibility_of_element_located((By.XPATH, "//li[@class='list-group-item border-0 px-0 d-flex']"))
                    ).text
                    tabela_de_vagas["modelo_contrato"].append(modelo_contratual)
                except:
                    modelo_contratual = "CLT"
                    tabela_de_vagas["modelo_contrato"].append(modelo_contratual)

                # Busca e atribui os Beneficios da Vaga à tabela
                try:
                    beneficios = []
                    j = 1
                    while True:
                        try:
                            beneficio = WebDriverWait(navegador, 0.1).until(
                                EC.visibility_of_element_located((By.XPATH, f'/html/body/main/div/div/div[1]/div[3]/div[1]/div/div[2]/span[{j}]'))
                            ).text
                            beneficios.append(beneficio)
                            j += 1
                        except:
                            break
                    tabela_de_vagas["beneficios"].append(beneficios)
                except:
                    try:
                        beneficios = WebDriverWait(navegador, 0.1).until(
                            EC.visibility_of_element_located((By.XPATH, '/html/body/main/div/div/div[1]/div[3]/div[1]/div/p[5]'))
                        ).text
                        tabela_de_vagas["beneficios"].append(beneficios)
                    except:
                        break
            
                # Busca e atribui a Descrição da Vaga à tabela
                try:
                    descricao_cargos = []
                    
                    descricao_titulo = WebDriverWait(navegador, 0.1).until(
                        EC.visibility_of_element_located((By.XPATH, '/html/body/main/div/div/div[1]/div[3]/div[1]/div/p[1]'))
                    ).text
                    descricao_titulo = descricao_titulo + ":"
                    descricao_cargos.append(descricao_titulo)
                    j = 1
                    while True:
                        try:
                            descricao_cargo = WebDriverWait(navegador, 0.1).until(
                                EC.visibility_of_element_located((By.XPATH, f'/html/body/main/div/div/div[1]/div[3]/div[1]/div/div[1]/p[{j}]'))
                            ).text
                            descricao_cargos.append(descricao_cargo)
                            j += 1
                        except:
                            break

                    requisitos_titulo = WebDriverWait(navegador, 0.1).until(
                        EC.visibility_of_element_located((By.XPATH, '/html/body/main/div/div/div[1]/div[3]/div[1]/div/p[2]'))
                    ).text
                    requisitos_titulo = requisitos_titulo + ":"
                    descricao_cargos.append(requisitos_titulo)
                    k = 3
                    while True:
                        try:
                            requisitos = WebDriverWait(navegador, 0.1).until(
                                EC.visibility_of_element_located((By.XPATH, f'/html/body/main/div/div/div[1]/div[3]/div[1]/div/p[{k}]'))
                            ).text
                            descricao_cargos.append(requisitos)
                            k += 1       
                        except:
                            break
                
                    tabela_de_vagas["descricao_vaga"].append(descricao_cargos)
                except:
                    break
    
                # Atualiza a barra de progresso
                pbar.update(1)

                # Retorna o navegador para a página anterior e passa a iteração até a próxima vaga
                navegador.back()
                time.sleep(1)

                total_vagas_extraidas_2 += 1

            # Mensagem de erro e interrupção do loop em caso de falha
            except Exception as e:
                print(f"Erro na iteração {i}: {e} - Site: {site_name}")
                break

        # Mensagem de conclusão de vagas extraídas
        print(f"Busca por {nome} concluído. Total de Vagas Extraídas: {i}\n")

# Mensagem de confirmação de todas Vagas Extraídas com sucesso
print(f"Web Scraping de {site_name} finalizado - Vagas Extraídas: {total_vagas_extraidas_2}.\n")



#%%
# In[1.3]:




# %%
# In[2.0]: Salvando Dados Obtidos em um Banco de Dados com Formato .CSV

# Busca e atribuí o diretório do Project para salvar os arquivos
diretorio_projeto = os.getcwd()
print(f"Salvando os Dados extraídos em formato .csv no diretório: {diretorio_projeto}")

# Criação de um DataFrame do pandas a partir da tabela de vagas com os dados extraídos
df_tabela_de_vagas = pd.DataFrame(tabela_de_vagas)
df_tabela_de_vagas['beneficios'] = df_tabela_de_vagas['beneficios'].apply(lambda x: ', '.join(x))
df_tabela_de_vagas['descricao_vaga'] = df_tabela_de_vagas['descricao_vaga'].apply(lambda x: ' '.join(x))

# Criação de um arquivo .csv no diretório do Project que recebe as informações da tabela de vagas
csv_name = "vagas_cargo_de_dados.csv"
df_tabela_de_vagas.to_csv('vagas_scrap.csv', index=False, encoding='utf-8', sep=';')
print(f"Dados de Vagas Extraídos salvos em {csv_name} dentro do diretório: {diretorio_projeto}")


# %%
