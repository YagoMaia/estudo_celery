from tasks import add, hello, inserir
from time import sleep

#* Pra juntar isso com o FastAPI tem que colocar essa execução de tasks no background

soma = add.delay(66, 4)
print(soma.get())

hello_world = hello.delay()
print(hello_world.get())

setar_redis = inserir.delay("teste", "celery")
print(setar_redis.ready())
print(setar_redis.get())