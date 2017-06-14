import pika
import json
import redis
import uuid

def replace_unicode(text):
	map_letters = {
		' ': '_',
		'ą': 'a',
		'ć': 'c',
		'ę': 'e',
		'ł': 'l',
		'ó': 'o',
		'ź': 'z',
		'ż': 'z',
		'ń': 'n',
		'ś': 's'
	}

	for key in map_letters:
		text = text.replace(key, map_letters[key])
	return text

def parse_ingredients(html_data):
    import re

    data = html_data.replace('\n', '')

    title_regex = r'<title>(.*?)</title>'
    title = re.search(title_regex, data)
    print(title)
    if not title:
        return {'name': '', 'ingredients': []}
    title = title.group(0)
    title = re.sub('^<title>', '', title)
    title = re.sub('</title>$', '', title)

    pattern = r'<div class="field field-name-field-skladniki field-type-text-long field-label-hidden">.+?</div>\s*</div>'
    extracted_ingredients = re.search(pattern, data)

    ingredient_regex = r'\s*<li>\s*(.+?)\s*</li>\s*'
    if extracted_ingredients:
        res_list = list(re.findall(ingredient_regex, extracted_ingredients.group(0)))
        return {
            'name': replace_unicode(title),
            'ingredients': [{'name': replace_unicode(ingr)} for ingr in res_list]
        }

    return {'name': '', 'ingredients': []}

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='rabbit', port=5672))
channel = connection.channel()

channel.queue_declare(queue='taco')
channel.queue_bind(exchange='crawled_notifications', queue='taco')

r = redis.Redis(host='burrito', port=6379)

def callback(ch, method, properties, body):
    parsed_json = json.loads(body.decode('utf-8'))

    if 'przepis' in parsed_json['Uri']:
        extracted_data = parse_ingredients(parsed_json['Html'])
        dict_to_redis = {
            'uri': parsed_json['Uri'],
            'title': extracted_data['name']
        }
        json_to_redis = json.dumps(dict_to_redis)
        ingredients = [i['name'] for i in extracted_data['ingredients']]
        if ingredients:
            r.sadd('ingredients', *ingredients)
        
        for ingr in ingredients:
            recipe_id = str(uuid.uuid4())
            r.sadd(ingr, recipe_id)
            r.sadd('ids', recipe_id)
            r.set(recipe_id, json_to_redis)
            
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(callback,
                      queue='taco')

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
