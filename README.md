# Rotatividade de Funcionários (Turnover)

### Introdução

Neste projeto de Análise de Recursos Humanos, temos como objetivo responder questões-chave sobre gestão de talentos e rotatividade de colaboradores em uma empresa fictícia. Após configurar a infraestrutura, \
modelamos atributos relevantes a partir de fontes variadas: arquivos `xlsx`, `json` e um banco de dados `MySQL`. Em seguida, realizamos uma análise dos dados para identificar fatores de desligamento. 
No decorrer do projeto, desenvolvemos habilidades em `Data Science` e `Engenharia de Dados`. Como resultado final, criamos um aplicativo interativo no `Streamlit` para visualizar nossos insights e os 
resultados do modelo de `Machine Learning`, que determina se um colaborador pode deixar a empresa. Para isso, utilizamos tecnologias como `Apache Airflow`, `Docker` e `Minio` para automatizar fluxos de 
dados e empregamos ferramentas como `Pandas`, `Scikit-learn`, `Pycaret,` `SweetViz` para otimizar nossa análise. O aplicativo interativo fornece uma abordagem eficiente e abrangente para lidar com os desafios 
da gestão de recursos humanos e identificar possíveis candidatos a deixar a empresa.

### Overview

![overview2.png](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/5f3150ee-23dd-4704-bd07-342549d8f5de/overview2.png)

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

Criar containers para simular um ambiente real de producao

1. Criação container MySQL
2. Criacao Data Lake com MinIO Server
3. Criação container Airflow
4. Configuracao Data Lake (MinIO) 
5. Carregar os arquivos para seus respectivos diretorios

Passo a passo explicado: [01_criacao_containers.md](notion://www.notion.so/notebooks/01_make_dataset.ipynb)
