import random
from faker import Faker
import logging
import logging.config

# Faker::Config.locale = :en

logging.config.fileConfig("Properties/configuration/logging.config")
logger = logging.getLogger("Data_generation")

# Initializing an object for faker
fake_india = Faker('en_IN')
fake_US = Faker('en_US')

import requests

API_URL = 'https://randomuser.me/api/?nat=IN&age>=18'
PARTIES = ['Bharatiya Vikas Sangathan', 'Rashtriya Lok Adhikar Dal', 'Aam Nagrik Sangathan']

random.seed(21)


def generate_data_for_candidates(candidate_number, total_parties):
    logger.warning("Starting generating data for the candidates....")
    response = requests.get(API_URL + '&gender=' + ('female' if candidate_number % 2 == 1 else 'male'))
    if response.status_code == 200:
        logger.warning("Was able to fetch data from API.Fetching user data.....")
        user_data = response.json()['results'][0]

        name = f"{user_data['name']['first']} {user_data['name']['last']}"
        birth_date = user_data['dob']['date']
        birth_place = fake_india.city()
        education = fake_india.random_element(
            elements=("High School Diploma", "Bachelor's Degree", "Master's Degree", "PhD"))
        profession = fake_india.random_element(
            ["Lawyer", "Doctor", "Engineer", "Businessperson", "Teacher", "Social Worker"])
        # political_experience = fake.random_int(min=1, max=20)
        experience = fake_india.random_int(min=0, max=30)
        quote = fake_US.sentence()

        politician_biography = f"{name}  was born on {birth_date} in {birth_place}. They are representing the {PARTIES[candidate_number % total_parties]} in the upcoming elections. {name} holds a {education} and has a background as a {profession}. With {experience} years of experience in public service, they are dedicated to bringing positive change to their constituency. Their campaign slogan is: '{quote}'."

        logger.warning("Returning the fetched data....")
        return {
            'candidate_id': user_data['login']['uuid'],
            'candidate_name': f"{user_data['name']['first']} {user_data['name']['last']}",
            'party_affiliation': PARTIES[candidate_number % total_parties],
            'biography': politician_biography,
            'campaign_platform': f"-{generate_fake_platform()}",
            'photo_url': user_data['picture']['large']
        }
    else:
        return 'Error generating data'


def generate_fake_platform():
    logger.warning("Choosing a promise randomly....")
    campaign_promises = [
        "To implement measures to stimulate economic growth and job creation, especially in the wake of the COVID-19 pandemic.",
        "To provide comprehensive support to farmers, including debt relief, crop insurance, and improved access to markets.",
        "To invest in healthcare infrastructure and human resources to ensure accessible and quality healthcare services for all.",
        "To implement strict pollution control measures and conservation efforts to address air and water pollution, deforestation, and loss of biodiversity.",
        "To strengthen anti-corruption laws and institutions, promote transparency in governance, and streamline bureaucratic processes.",
        "To enhance national security measures to combat terrorism, insurgency, and cyber threats, and ensure the safety of citizens.",
        "To implement policies to address caste-based discrimination, promote social harmony, and ensure equal rights and opportunities for all.",
        "To develop sustainable urban planning strategies, improve infrastructure, and provide affordable housing to address urban overcrowding and slum proliferation.",
        "To improve access to quality education for all, bridge the urban-rural education divide, and expand vocational training opportunities to reduce unemployment among youth.",
        "To pursue diplomatic dialogue and conflict resolution with neighboring countries, and strengthen international cooperation to address global challenges."
    ]
    return fake_india.random_element(campaign_promises)


generated_user_ids = set()


def generate_data_for_voters():
    logger.warning("Starting generating data for the voters....")
    response = requests.get(API_URL)
    # voterids = set()

    if response.status_code == 200:
        logger.warning("Was able to fetch data from API.Fetching user data.....")
        user_data = response.json()['results'][0]
        # voterids.add(user_data['login']['uuid'])

        # Generate a unique candidate ID
        voter_id = generate_unique_id()
        while voter_id in generated_user_ids:
            voter_id = generate_unique_id()
        generated_user_ids.add(voter_id)

        logger.warning("Returning the voters' data....")
        return {
            'voter_id': voter_id,
            'voter_name': f"{user_data['name']['first']} {user_data['name']['last']}",
            'date_of_birth': user_data['dob']['date'],
            'gender': user_data['gender'],
            'nationality': user_data['nat'],
            'registration_number': user_data['login']['username'],
            'address': {
                'street': f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                'city': user_data['location']['city'],
                'state': user_data['location']['state'],
                'country': user_data['location']['country'],
                'postcode': user_data['location']['postcode']
            },
            'email': user_data['email'],
            'phone_number': user_data['phone'],
            'picture': user_data['picture']['large'],
            'registered_age': user_data['registered']['age']
        }


def generate_unique_id():
    return fake_india.uuid4()
