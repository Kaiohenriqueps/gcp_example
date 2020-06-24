# PRÉ-REQUISITOS
* instalar python3
* instalar pip3
* instalar google-cloud-storage
* instalar google-cloud-bigquery
* exportar variável
```
$ export GOOGLE_APPLICATION_CREDENTIALS="credentials.json"
```

# PASSO A PASSO
1) Fazer upload dos arquivos para o Cloud Storage
```
$ python3 src/wrappers/storage_wrapper.py
```

2) Criação do dataset, das tabelas e query
```
$ python3 src/wrappers/bigquery_wrapper.py
```