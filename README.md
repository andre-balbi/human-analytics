# Rotatividade de Funcionários (Turnover)

### Introdução

Neste projeto de Análise de Recursos Humanos, temos como objetivo responder questões-chave sobre gestão de talentos e rotatividade de colaboradores em uma empresa fictícia. Após configurar a infraestrutura, \
modelamos atributos relevantes a partir de fontes variadas: arquivos `xlsx`, `json` e um banco de dados `MySQL`. Em seguida, realizamos uma análise dos dados para identificar fatores de desligamento. 
No decorrer do projeto, desenvolvemos habilidades em `Data Science` e `Engenharia de Dados`. Como resultado final, criamos um aplicativo interativo no `Streamlit` para visualizar nossos insights e os 
resultados do modelo de `Machine Learning`, que determina se um colaborador pode deixar a empresa. Para isso, utilizamos tecnologias como `Apache Airflow`, `Docker` e `Minio` para automatizar fluxos de 
dados e empregamos ferramentas como `Pandas`, `Scikit-learn`, `Pycaret,` `SweetViz` para otimizar nossa análise. O aplicativo interativo fornece uma abordagem eficiente e abrangente para lidar com os desafios 
da gestão de recursos humanos e identificar possíveis candidatos a deixar a empresa.

### Overview

![overview2.png](img/overview2.png)

### Objetivos

- Entender quais são os fatores que influenciam para um colaborador desejar deixar a empresa
- Antecipar e saber se um determinado colaborador vai sair da empresa

### Instalação

Criar, instalar e ativar o ambiente  Anaconda: 

```
conda env create -f environment.yml.
```

---

### **Etapa 01 - Criação dos Containers**

Criar containers simulando um ambiente real de produção.

#### MySQL Server

1. Criar o container do MySQL habilitando a porta 3307 executando o comando via terminal Powershell:
    
```
docker run --name mysqlbd1 -e MYSQL_ROOT_PASSWORD=0000 -p "3307:3306" -d mysql
```
    

> Host: 127.0.0.1
Username: root
Port: 3307
Password: 0000
> 

![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/3dbda08c-70ff-4522-96de-e48781f464ca/Untitled.png)

## Data Lake com Minio Server

1. Dentro do diretório projeto crie o diretório ***datalake***.
2. Execute o comando via terminal Powershell:
    
    `docker run --name minio -d -p 9000:9000 -p 9001:9001 -v "$PWD/datalake:/data" minio/minio server /data --console-address ":9001”`
    

> [http://localhost:9000](http://localhost:9000/login)
username: minioadmin
password: minioadmin
> 

## Airflow

1. Dentro do diretório projeto criar o diretório ***airflow***.
2. Navegar até o diretório airflow e criar o diretório ***dags***.
3. Execute o comando via terminal Powershell:

`docker run -d -p 8080:8080 -v "$PWD/airflow/dags:/opt/airflow/dags/" --entrypoint=/bin/bash --name airflow apache/airflow:2.1.1-python3.8 -c '(airflow db init&& airflow users create --username admin --password 0000 --firstname Andre --lastname Lastname --role Admin --email [admin@example.org](mailto:admin@example.org)); airflow webserver & airflow scheduler'`

> [https://localhost:8080](https://localhost:8080/)
Login: admin
Senha: stack
> 
1. Instalar as bibliotecas necessárias para o ambiente:
    1. Execute o comando para se conectar ao container do airflow: 
        
        `docker container exec -it airflow bash`
        
    2. Instalar as bibliotecas:
        
        `pip install pymysql xlrd openpyxl minio`
        
2. Criar as variáveis em ***Admin > Variables***

![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/d435a74b-0cba-46b1-a05f-e1ddfbf0b24a/Untitled.png)

> data_lake_server = 172.17.0.3:9000
data_lake_login = minioadmin
data_lake_password = minioadmin
database_server = 172.17.0.2 (executar `docker container inspect mysqlbd1` e localizar o atributo ***IPAddress***)
database_login = root
database_password = 0000
database_name = employees
> 

# Etapa 02 - Criação dos Dados

## Configurando o Data Lake

1. Criar os buckets ***landing, processing*** e ***curated.***

![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/b4fb6018-6a4e-4e6f-9780-fb334eb074a2/Untitled.png)

1. No bucket ***landing***, criar a pasta ***performance-evaluation***, nela será inserido o arquivo json referente a avaliação individual do desempenho dos funcionários realizados pela empresa. 
2. Ainda no bucket ***landing***, criar outra pasta chamada ***working-hours,*** nela serão inseridos arquivos .xlsx referente as horas trabalhadas pelos funcionários.

![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/fead4ab2-03a8-45e7-a33e-5042dbbd4b6b/Untitled.png)

## Subindo e carregando o banco de dados

1. Importar arquivo ***employees_db.sql*** contendo o script para criação do banco de dados (localizado na pasta ***database*** do projeto)
