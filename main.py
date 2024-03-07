import tasks
from fastapi import FastAPI
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

@app.post("/group")
def task_group_soma():
    tasks_list = []
    data = []
    
    for b in range(0,5):
        tasks_list.append(tasks.add.s(b, b+2))
    
    job = group(tasks_list)
    
    result = job.apply_async()
    ret_values = result.get(disable_sync_subtasks=False)
    for r in ret_values:
        data.append(r)
    return data

@app.get("/task/{task_id}")
def get_task(task_id):
    ret = tasks.get_task_by_id(task_id)
    return {
        'Id' : task_id,
        'Status': ret.status,
        'Result': ret.get()
    }
    
# soma = add.delay(66, 4)
# print(soma.get())

# hello_world = hello.delay()
# print(hello_world.get())

# setar_redis = inserir.delay("teste", "celery")
# print(setar_redis.ready())
# print(setar_redis.get())