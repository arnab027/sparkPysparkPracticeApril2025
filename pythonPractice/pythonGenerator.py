import logging

logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def generate_number(number_limit:int)->iter:
    if isinstance(number_limit,int) and number_limit > 0:
        for numbers_between in range(0, number_limit):
            logger.info(msg="Inside generate_number")
            yield numbers_between


def call_generator_function(number_limit:int)->iter:
    generator_calling_instance = generate_number(number_limit)
    if next(generator_calling_instance) <number_limit:
        logger.info(msg="Inside call_generator_function")
        yield from generator_calling_instance

values_fetched = call_generator_function(6)
generator_val = iter(values_fetched)

while True:
    try:
        value = next(generator_val)
    except StopIteration:
        break
    print(value)