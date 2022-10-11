
"""  Redis Modülünü İmport Ediyoruz """
import redis
""" Redis baglanıyoruz host yazan yere IP nizi Yazın"""
r = redis.StrictRedis(host="localhost", port=6379, password=None)

""" Boru hatti pipeline olusturarak toplu bir şeklilde execute ediyoruz 
Böylece Daha hızlı bir şekilde insert gerçekleştiriyoruz
"""
pipe1=r.pipeline() 
reply = pipe1.execute_command ('GRAPH.QUERY', 'social1', "CREATE (:person {name:'Ramazan', age:27, gender:'male', status:'single'})")

pipe1.execute()



