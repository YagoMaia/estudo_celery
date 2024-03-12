from celery import Celery, Task
from celery.result import AsyncResult
import redis
import os

redis_connection = redis.Redis()

os.environ.setdefault('FORKED_BY_MULTIPROCESSING', '1')
BROKER_URL = 'redis://localhost:6379/1'
BACKEND_URL = 'redis://localhost:6379/1'

#* celery -A tasks worker -l info -P gevent
#* celery -A tasks worker --pool=solo -l info

class DebugTask(Task):
    """
    Classes responsável por informar sobre a Task
    """
    def __call__(self, *args, **kwargs):
        print('TASK STARTING: {0.name}[{0.request.id}]'.format(self))
        return self.run(*args, **kwargs)

#* Inicializando o Celery
celery_app = Celery('tasks', broker=BROKER_URL, backend=BACKEND_URL, broker_connection_retry_on_startup = True)

# @celery_app.on_after_configure.connect
# def setup_periodic_tasks(sender, **kwargs):
#     # Calls test('hello') every 10 seconds.
#     senderedis_connection.add_periodic_task(10.0, hello.s(), name='add every 10')

@celery_app.task(name = "Soma de dois números", base = DebugTask)
def add(x: int, y: int) -> int:
    """
    Função responsável por somar dois números
    
    x: Primeiro número da soma
    y: Segundo número da soma
    """
    return x + y

@celery_app.task(name = "Hello World", base = DebugTask, ignore_result = True)
def hello() -> str:
    """
    Função responsável por retornar hello world
    """
    return "Hello World"

@celery_app.task(name = "Inserir Valor redis", base = DebugTask)
def inserir(chave, valor) -> bool:
    """
    Função responsável por setar um par chave-valor no redis
    
    chave: Chave que será setada no redis
    valor: Valor que será associado a chave
    """
    ret = redis_connection.hset("celery_fastapi", chave, valor)
    if ret:
        return True
    return False

@celery_app.task(name = "Deletar campo redis", base = DebugTask)
def deletar(chave) -> bool:
    """
    Função responsável por deletar um campo  associado a uma chave no redis
    
    chave: Chave que será deletada no redis
    """
    ret = redis_connection.hdel("celery_fastapi", chave)
    if ret:
        return True
    return False

@celery_app.task(name = "Limpar Cache redis", base = DebugTask)
def flush_redis() -> str:
    """
    Função responsável por dar um flushdb no redis
    """
    ret = redis_connection.flushdb()
    if ret:
        return "Chaves dos Redis limpas"
    return "Erro ao limpar chaves"

def get_task_by_id(task_id) -> AsyncResult:
    """
    Função responsável por pegar os dados de um tarefa no celery, dado seu id
    
    task_id : Id da Task do celery
    """
    ret = celery_app.AsyncResult(task_id)
    return ret

#TODO Estudar -> Linking (callbacks/errbacks) 
#TODO Estudar -> ETA and Countdown
#TODO Estudar -> Routing options add.apply_async(queue='priority.high')