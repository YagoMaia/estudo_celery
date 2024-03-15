import tasks
from fastapi import FastAPI, BackgroundTasks, Form
from celery.result import AsyncResult
from celery import group

#* Pra juntar isso com o FastAPI tem que colocar essa execução de tasks no background

app = FastAPI()

@app.get("/")
def home() -> str:
    """
    Endpoint responsável por gerar a Home Page
    """
    return "Home Page"

@app.post("/soma")
def task_soma(num1 : int = Form(), num2 : int = Form()) -> AsyncResult.id:
    """
    Endpoint responsável por somar dois números, feito pelo celery
    """
    soma1 = tasks.add.delay(num1, num2)
    soma2 = tasks.add.apply_async((num1, num2), link_error=tasks.error_handler.s())
    soma3 = tasks.add.apply_async((num1, num2), retry = False, queue = 'priority.high') #Necessário colocar no argumento ao iniciar o celery
    return soma1.id, soma2.id, soma3.id

@app.post("/hello_world")
def hello_world() -> AsyncResult.id:
    """
    Endpoint responsável por retornar Hello World, feito pelo celery
    """
    #hello = tasks.hello.delay()
    try:
        hello = tasks.hello.apply_async(())
    except hello.OperationalError as exec:
        print(exec)
    return hello.id

@app.post("/inserir_redis")
def inserir_dados_redis(chave: str = Form(), valor: str | int = Form()) -> AsyncResult.id:
    """
    Endpoint responsável por inserir um par chave-valor no redis, feito pelo celery
    """
    setar_redis = tasks.inserir.delay(chave, valor)
    return setar_redis.id

@app.post("/apagar_campo_redis")
def apagar_campo_redis(chave : str = Form()) -> AsyncResult.id:
    """
    Endpoint responsável por deletar um campo associado a uma chave no redis, feito pelo celery
    """
    deletar_campo = tasks.deletar.delay(chave)
    return deletar_campo.id

#TODO Apagar Endpoints
#* Ter a opção de limpar todas as rotas ou de limpar uma rota específica seria bom
#* A função de apagar TODOS os endpoints existentes lá seria fácil, só dar um flushdb
#* Para apagar de uma universidade também seria fácil, seria só apagar a chave que tem o id_ies dela
#* Para apagar um endpoint especifício poderia pegar todas os campos que estão vinculados a universidade e apagar o campo
#* Teria endpoints repetidos, mudando apenas o parâmetro na URL
#* Olhar se o redis tem alguma função tipo o like do Postgres aí daria pra apagar todos os endpoints, mesmo com as variações de parâmetros

#TODO Inserir Endpoints
#? A parte de inserir é a que complica
#? A parte de inserir não seria literalmente acessar o endpoint via docs do fastapi ou chamar um requisão normal ??
#? A questão de recolocar os endpoints no redis seria a questão dos parâmetros...
#? Algumas opções seriam acessar o sistema normal, mas pra uma grande quantidade de universidades não faria sentido
#? Outra opção mais viável seria criar um módulo que faria as principais requisições...

@app.post("/background/inserir_redis")
def inserir_dados_redis(*, chave: str = Form(), valor: str | int = Form(), background_task : BackgroundTasks) -> str:
    """
    Endpoint responsável por inserir um par chave-valor em que é feito em Background Task pelo celery
    """
    try:
        background_task.add_task(tasks.inserir.delay, chave, valor)
        return "Função inserir dados no redis sendo executada em Background"
    except Exception:
        return "Erro ao executar função inserir dados em Background"

@app.post("/group")
def task_group_soma() -> str:
    """
    Endpoint responsável por realizar a soma de dois números com o método group do celery.
    """
    tasks_list = []
    data = []
    
    for b in range(0,5):
        tasks_list.append(tasks.add.s(b, b+2)) #! .s Seria o que exatamente?
    
    job = group(tasks_list)
    
    result = job.apply_async()
    return "Task executada"
    #ret_values = result.get(disable_sync_subtasks=False)
    #for r in ret_values:
    #    data.append(r)
    #return data

@app.get("/task/{task_id}")
def get_task(task_id : str) -> dict:
    """
    Endpoint responsável por retoranr os dados de uma task do celery.
    """
    ret = tasks.get_task_by_id(task_id)
    return {
        'Id' : task_id,
        'Status': ret.status,
        'Result': ret.get()
    }
    
@app.post("/inserir_db")
def inserir_usuario_banco_de_dados(*, id : str = Form(), name : str = Form(), background_task : BackgroundTasks):
    try:
        background_task.add_task(tasks.inserir_banco_dados.delay, id, name)
        return "Inserindo novo usuário no banco de dados via BackGround"
    except Exception as error:
        print(error)
        return "Erro ao inserir usuário no banco de dados via BackGround"