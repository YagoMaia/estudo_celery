from celery import Celery, Task
import redis
import os
from requests import Session, request

URL = "http://localhost:8000"

r = redis.Redis()

os.environ.setdefault('FORKED_BY_MULTIPROCESSING', '1')

class DebugTask(Task):

    def __call__(self, *args, **kwargs):
        print('TASK STARTING: {0.name}[{0.request.id}]'.format(self))
        return self.run(*args, **kwargs)

#* celery -A tasks worker -l info -P gevent

BROKER_URL = 'redis://localhost:6379/1'
BACKEND_URL = 'redis://localhost:6379/1'
celery_app = Celery('tasks', broker=BROKER_URL, backend=BACKEND_URL, broker_connection_retry_on_startup = True)

@celery_app.task(name = "Soma de dois números", base = DebugTask)
def add(x, y):
    return x + y

@celery_app.task(name = "Hello World", base = DebugTask)
def hello():
    return "Hello World"

@celery_app.task(name = "Inserir Valor redis", base = DebugTask)
def inserir(chave, valor):
    ret = r.set(chave, valor)
    if ret:
        return True
    return False

@celery_app.task(name = "Limpar Cache redis", base = DebugTask)
def flush_redis():
    ret = r.flushdb()
    if ret:
        return "Chaves dos Redis limpas"
    return "Erro ao limpar chaves"

#* Celery deve só funcionar para funções básicas e funções que envolvam o redis
@celery_app.task(name = "Request Sistema", base = DebugTask)
def request_sistema(path:str, method:str):
    if method == "GET":
        ret = request(method, f"{URL}{path}")
        return ret.status_code
        if ret:
            return True
        return False

#* Pode fazer assim deixando os métodos relacioandos a celery tudo em um arquivo só...
def get_task_by_id(task_id):
    ret = celery_app.AsyncResult(task_id)
    return ret