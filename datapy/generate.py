import random

from faker import Faker

if __name__ == '__main__':
    fake = Faker()
    with open('data.csv', 'w') as output_file:
        for _ in range(100):
            name = fake.name()
            age = random.randint(18, 105)
            job = fake.job()
            phone = fake.phone_number()
            # print(age, name, job, phone)
            output_file.write('|'.join(str(x) for x in [age, name, job, phone]) + '\n')
