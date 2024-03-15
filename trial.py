import requests
from faker import Faker

# Initializing an object for faker
fake_US = Faker('en_US')
fake_india = Faker('en_IN')


def generate_fake_biography_and_promises():
    politician_name = fake_india.name()
    party_name = fake_india.company()
    birth_place = fake_india.city()

    # biography = fake.paragraphs(nb=1)[0]
    education = fake_india.random_element(
        elements=("High School Diploma", "Bachelor's Degree", "Master's Degree", "PhD"))
    profession = fake_india.random_element(
        ["Lawyer", "Doctor", "Engineer", "Businessperson", "Teacher", "Social Worker"])
    # political_experience = fake.random_int(min=1, max=20)
    experience = fake_US.random_int(min=0, max=30)
    quote = fake_india.sentence()

    politician_biography = f"{politician_name}  was born in {birth_place}. They are representing the {party_name} in the upcoming elections. {politician_name} holds a {education} and has a background as a {profession}. With {experience} years of experience in public service, they are dedicated to bringing positive change to their constituency. Their campaign slogan is: '{quote}'."

    campaign_promises = []
    for _ in range(fake_US.random_int(min=3, max=6)):
        promise = {
            "issue": fake_US.sentence(nb_words=3),
            "solution": fake_US.paragraph(nb_sentences=2),
            "priority": fake_US.random_element(elements=('High', 'Medium', 'Low'))
        }
        campaign_promises.append(promise)

    return politician_name, party_name, politician_biography, campaign_promises


# politician_name, party_name, biography, campaign_promises = generate_fake_biography_and_promises()
url = 'https://randomuser.me/api/?nat=IN&age>=18'
response = requests.get(url)
user_data = response.json()['results'][0]
print(user_data)
