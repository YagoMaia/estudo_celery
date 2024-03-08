import tasks
from fastapi import FastAPI, BackgroundTasks
from celery.result import AsyncResult
from celery import group

#* Pode fazer uma função puxando o app do celery para o endpoint, creio que não seria bom...
# def get_task_by_id(task_id):
#     ret = tasks.celery_app.AsyncResult(task_id)
#     return ret

#* Pra juntar isso com o FastAPI tem que colocar essa execução de tasks no background

app = FastAPI()

@app.get("/")
def home():
    return "Home Page"

@app.post("/soma")
def task_soma(num1 : int, num2 : int):
    soma = tasks.add.delay(num1, num2)
    return soma.id

@app.post("/hello_world")
def hello_world():
    hello = tasks.hello.delay()
    return hello.id

@app.post("/inserir_redis")
def inserir_dados_redis(chave, valor):
    setar_redis = tasks.inserir.delay(chave, valor)
    return setar_redis.id

#* Tentar deixar nessa estrutura await kwargs['redis'].setField(universidade, path, ret)
#* Criar uma tabela contendo os endpoints? -> E se mudar? muito trabalho
#* Eu precisaria do path independente
#* Precisaria do id_ies, do path, e do retorno
#* Uso requests para acessar os endpoints?
#* Daí faço uma dict contendo o path e o retorno?
#* Precisaria de uma lista com todos os endpoint no minimo
#* Valeria a pena reinserir no redis as principais rotas, tipo aquelas que mais usam que não usam parametros na url
#* Ter a opção de limpar todas as rotas ou de limpar uma rota específica seria bom

@app.post("/background/inserir_redis")
def inserir_dados_redis(chave, valor, background_task : BackgroundTasks):
    try:
        background_task.add_task(tasks.inserir.delay, chave, valor)
        return "Função inserir dados no redis sendo executada em Background"
    except Exception:
        return "Erro ao executar função inserir dados em Background"

@app.post("/group")
def task_group_soma():
    tasks_list = []
    data = []
    
    for b in range(0,5):
        tasks_list.append(tasks.add.s(b, b+2)) #.s Seria o que exatamente?
    
    job = group(tasks_list)
    
    result = job.apply_async()
    ret_values = result.get(disable_sync_subtasks=False)
    for r in ret_values:
        data.append(r)
    return data

# @app.post("/group/requests")
# def task_group_requests(background_task : BackgroundTasks):
    
#     try:
#         tasks_list = []
#         #data = []
#         lista_rotas = retorna_rotas()
        
#         #for r in lista_rotas:
#         #    tasks_list.append(tasks.request_sistema.s(r['path'], r['method'])) #.s Seria o que exatamente?
        
#         #job = group(tasks_list)
        
#         #result = job.apply_async()
#         for r in lista_rotas:
#             background_task.add_task(tasks.request_sistema.delay, r['path'], r['method'])
        
#         return "Executando a função em background"
#         #ret_values = result.get(disable_sync_subtasks=False)
#         #for r in ret_values:
#         #    data.append(r)
#         #return data
#     except Exception as error:
#         return "Erro ao executar função em background"

@app.get("/task/{task_id}")
def get_task(task_id):
    ret = tasks.get_task_by_id(task_id)
    return {
        'Id' : task_id,
        'Status': ret.status,
        'Result': ret.get()
    }

#* Não é todas as rotas que devem ser inseridas no redis
#! Isso daqui não vai dar certo
# @app.get("/rotas")
# def retorna_rotas():
#     rotas_lista = []
#     for r in app.routes:
#         if "HEAD" not in r.methods and "GET" in r.methods and "/rotas" not in r.path and "{" not in r.path:
#             for m in r.methods:
#                 rotas_lista.append({'path':r.path, 'name': r.name, 'method' : m})
#     return rotas_lista