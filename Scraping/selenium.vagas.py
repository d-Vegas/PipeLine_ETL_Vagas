import os
import csv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time

# Configurar o Brave como navegador
brave_path = "C:/Program Files/BraveSoftware/Brave-Browser/Application/brave.exe"  # Atualize o caminho do Brave

# Configurar o WebDriver para usar o Brave
options = webdriver.ChromeOptions()
options.binary_location = brave_path

# Inicializar o WebDriver com o Brave
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

# Lista de URLs de vagas para Analista de Dados, Engenheiro de Dados e Cientista de Dados
urls_vagas = [
    ("Análise de Dados", "https://www.vagas.com.br/vagas-de-Analista-de-dados"),
    ("Engenharia de Dados", "https://www.vagas.com.br/vagas-de-Engenheiro-de-dados"),
    ("Ciência de Dados", "https://www.vagas.com.br/vagas-de-Cientista-de-dados")
]

# Função para lidar com popups (se necessário)
def fechar_popup():
    try:
        # Tente encontrar o botão de fechar o popup pelo XPath ou seletor adequado
        popup_close_button = driver.find_element(By.XPATH, '//*[@class="close-popup-class"]')  # Ajuste o XPath
        popup_close_button.click()
        print("Popup fechado")
    except:
        print("Nenhum popup encontrado")

# Função para clicar no botão "Mostrar mais vagas" até ele desaparecer
def clicar_mostrar_mais_vagas():
    while True:
        try:
            # Tentar encontrar e clicar no botão "Mostrar mais vagas"
            mostrar_mais = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="maisVagas"]')))
            mostrar_mais.click()
            print("Clicou no botão 'Mostrar mais vagas'")
            time.sleep(2)  # Pausa para evitar sobrecarga no servidor
        except:
            print("Todas as vagas foram carregadas ou não há mais o botão 'Mostrar mais vagas'.")
            break

# Função para extrair todas as informações de vagas visíveis
def extrair_vagas(titulo_padronizado):
    vagas = driver.find_elements(By.XPATH, '//*[@id="todasVagas"]/ul/li')
    
    # Conjunto para armazenar dados únicos temporariamente
    dados_vagas = set()
    
    for vaga in vagas:
        try:
            # Extrair o título e padronizar com base na categoria
            titulo = vaga.find_element(By.XPATH, './/header/div[2]').text.strip()

            # Verificar se é um estágio
            if "Estágio" in titulo:
                nivel_experiencia = "Estágio"
            else:
                nivel_experiencia = identificar_nivel_experiencia(titulo)
            
            # Padronizar o título conforme solicitado
            titulo_simplificado = titulo_padronizado

            # Extrair a empresa
            try:
                empresa = vaga.find_element(By.XPATH, './/header/div[2]/span').text.strip()
            except:
                empresa = 'N/A'

            # Extrair a localização e simplificar para apenas a cidade
            localizacao = vaga.find_element(By.XPATH, './/footer/span[1]').text.strip()
            cidade = extrair_cidade(localizacao)

            # Extrair a data de publicação
            data_publicacao = vaga.find_element(By.XPATH, './/footer/span[2]').text.strip()

            # Adicionar campos extras (Salário e Modelo de Contrato)
            salario = "A combinar"
            modelo_contrato = "Regime CLT"

            # Criar uma tupla com os dados para evitar duplicatas
            dados_tuple = (titulo_simplificado, empresa, cidade, data_publicacao, nivel_experiencia, salario, modelo_contrato)

            # Adicionar os dados ao conjunto (conjunto só permite elementos únicos)
            dados_vagas.add(dados_tuple)

        except Exception as e:
            print(f"Erro ao processar uma vaga: {e}")

    return dados_vagas

# Função para identificar o nível de experiência com base no título
def identificar_nivel_experiencia(titulo):
    titulo_lower = titulo.lower()
    if "sênior" in titulo_lower or "senior" in titulo_lower:
        return "Sênior"
    elif "pleno" in titulo_lower:
        return "Pleno"
    elif "júnior" in titulo_lower or "junior" in titulo_lower:
        return "Júnior"
    elif "trainee" in titulo_lower:
        return "Trainee"
    else:
        return "Pleno"  # Se não houver, assume que é "Pleno"

# Função para extrair apenas a cidade (sem UF)
def extrair_cidade(localizacao):
    # Supondo que a localização esteja no formato "Cidade - UF"
    return localizacao.split(" - ")[0]

# Iniciar o arquivo CSV e incluir todas as buscas
with open('vagas_dados.csv', 'w', newline='', encoding='utf-8-sig') as csvfile:
    fieldnames = ['id_vaga', 'titulo_vaga', 'empresa', 'cidade', 'data_publicacao', 'nivel_experiencia', 'salario', 'modelo_contrato']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')
    writer.writeheader()

    # Contador para gerar IDs únicos
    id_contador = 1

    # Iterar sobre cada URL (Analista de Dados, Engenheiro de Dados e Cientista de Dados)
    for titulo_padronizado, url in urls_vagas:
        print(f"Abrindo a URL: {url} para {titulo_padronizado}")
        driver.get(url)
        
        # Esperar a página carregar
        wait = WebDriverWait(driver, 10)
        
        # Clicar no botão "Mostrar mais vagas" até que todas as vagas sejam carregadas
        clicar_mostrar_mais_vagas()

        # Extrair todas as vagas visíveis
        dados_vagas = extrair_vagas(titulo_padronizado)

        # Escrever os dados extraídos no CSV, garantindo que não haja duplicatas
        for vaga in dados_vagas:
            writer.writerow({
                'id_vaga': id_contador,
                'titulo_vaga': vaga[0],
                'empresa': vaga[1],
                'cidade': vaga[2],
                'data_publicacao': vaga[3],
                'nivel_experiencia': vaga[4],
                'salario': vaga[5],
                'modelo_contrato': vaga[6]
            })
            id_contador += 1  # Incrementar o ID para cada vaga

# Fechar o navegador
driver.quit()
print("CSV gerado com sucesso!")
