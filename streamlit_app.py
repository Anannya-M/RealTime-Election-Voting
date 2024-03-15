import time

import pandas as pd
import psycopg2
import streamlit
from streamlit_autorefresh import st_autorefresh
from kafka import KafkaConsumer
import simplejson as json
import matplotlib.pyplot as plt
from matplotlib import cm
import numpy as np


# streamlit.write("Realtime Election Voting")
@streamlit.cache_data
def fetch_voting_stats():
    conn = psycopg2.connect('host=localhost, dbname=votedb user=postgres password=root')
    curr = conn.cursor()

    print("Fetching total number of voters....")
    curr.execute(
        """
            SELECT COUNT(*) voters_count from voters
        """
    )
    total_voters = curr.fetchone()[0]

    print("Fetching total number of candidates....")
    curr.execute(
        """
            SELECT COUNT(*) candidates_count from candidates
        """
    )
    total_candidates = curr.fetchone()[0]

    return total_voters, total_candidates


def create_kafka_consumer(topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda res: json.loads(res.decode('utf-8'))
    )
    return consumer


def fetch_data_from_kafka(consumer):
    messages = consumer.poll(timeout_ms=1000)
    data = []
    for message in messages.values():
        for submsg in message:
            data.append(submsg.value)

    return data


def create_bar_chart(results):
    datatype = results['candidate_name']
    viridis = cm.get_cmap('viridis', len(datatype))
    colours = viridis(np.linspace(0, 1, len(datatype)))
    plt.bar(datatype, results['total_votes'], color=colours)

    plt.xlabel("Candidate")
    plt.ylabel("Total Votes Secured")
    plt.title("Votes Secured Per Candidate")
    plt.xticks(rotation=90)
    return plt


def create_pie_chart(results):
    labels = list(results['candidate_name'])
    size = list(results['total_votes'])
    fig, ax = plt.subplots()
    ax.pie(size, labels=labels, autopct='%1.1f%%', startangle=140)
    ax.axis('equal')

    plt.title("Votes Proportion")
    return fig


# Function to split a dataframe into chunks for pagination
@streamlit.cache_data(show_spinner=False)
def split_frame(input_df, rows):
    df = [input_df.loc[i: i + rows - 1, :] for i in range(0, len(input_df), rows)]
    return df


# Function to paginate a table
def paginate_table(table_data):
    top_menu = streamlit.columns(3)
    with top_menu[0]:
        sort = streamlit.radio("Sort Data", options=["Yes", "No"], horizontal=1, index=1)
    if sort == "Yes":
        with top_menu[1]:
            sort_field = streamlit.selectbox("Sort By", options=table_data.columns)
        with top_menu[2]:
            sort_direction = streamlit.radio(
                "Direction", options=["⬆️", "⬇️"], horizontal=True
            )
        table_data = table_data.sort_values(
            by=sort_field, ascending=sort_direction == "⬆️", ignore_index=True
        )
    pagination = streamlit.container()

    bottom_menu = streamlit.columns((4, 1, 1))
    with bottom_menu[2]:
        batch_size = streamlit.selectbox("Page Size", options=[10, 25, 50, 100])
    with bottom_menu[1]:
        total_pages = (
            int(len(table_data) / batch_size) if int(len(table_data) / batch_size) > 0 else 1
        )
        current_page = streamlit.number_input(
            "Page", min_value=1, max_value=total_pages, step=1
        )
    with bottom_menu[0]:
        streamlit.markdown(f"Page **{current_page}** of **{total_pages}** ")

    pages = split_frame(table_data, batch_size)
    pagination.dataframe(data=pages[current_page - 1], use_container_width=True)


def sidebar():
    if streamlit.session_state.get('latest_update') is None:
        streamlit.session_state['last_update'] = time.time()

    refresh_interval = streamlit.sidebar.slider("Refresh interval in seconds", 5, 60, 10)
    st_autorefresh(interval=refresh_interval * 1000, key='auto')

    if streamlit.sidebar.button("Refresh"):
        data_updation()


def check_voting_completion():
    conn = psycopg2.connect('host=localhost dbname=votedb user=postgres password=root')
    curr = conn.cursor()

    # Count the number of votes received
    curr.execute("SELECT COUNT(*) FROM votes")
    votes_count = curr.fetchone()[0]

    # Count the number of registered voters
    curr.execute("SELECT COUNT(*) FROM voters")
    voters_count = curr.fetchone()[0]

    conn.close()

    # Compare votes count and voters count
    if votes_count >= voters_count:
        return True
    else:
        return False


def data_updation():
    global election_ended

    last_refresh = streamlit.empty()
    last_refresh.text(f"Last refreshed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    print("Obtaining the statistics of the voting system....")
    total_voters, total_candidates = fetch_voting_stats()

    print("Displaying all the statistics about voting.....")
    streamlit.markdown("---")

    # To show the results (total voters & total candidates) in the form of 2 columns
    column1, column2 = streamlit.columns(2)
    column1.metric("Total Voters", total_voters)
    column2.metric("Total Candidates", total_candidates)

    # To refresh the dashboard and bring in data as its been generated
    consumer = create_kafka_consumer("aggregated_votes_per_candidate")
    data = fetch_data_from_kafka(consumer)
    result = pd.DataFrame(data)

    # Display the winner
    result = result.loc[result.groupby('candidate_id')['total_votes'].idxmax()]
    top_candidate = result.loc[result['total_votes'].idxmax()]
    streamlit.markdown("---")
    streamlit.header("Winning Candidate")
    column1, column2 = streamlit.columns(2)
    with column1:
        streamlit.image(top_candidate['photo_url'], width=200)
    with column2:
        streamlit.header(top_candidate['candidate_name'])
        streamlit.subheader(top_candidate['party_affiliation'])
        streamlit.subheader(f"Total Votes {top_candidate['total_votes']}")

    # Displaying the rest of the visualizations
    streamlit.markdown("---")
    streamlit.header("Related Statistics")
    results = result[['candidate_id', 'candidate_name', 'party_affiliation', 'total_votes']]
    results = results.reset_index(drop=True)

    column1, column2 = streamlit.columns(2)
    with column1:
        bar_chart = create_bar_chart(results)
        streamlit.pyplot(bar_chart)

    with column2:
        pie_chart = create_pie_chart(results)
        streamlit.pyplot(pie_chart)

    streamlit.table(results)

    # Fetching the locations
    consumer_location = create_kafka_consumer("aggregated_turnout_by_location")
    location = fetch_data_from_kafka(consumer_location)
    location_result = pd.DataFrame(location)

    location_result = location_result.loc[location_result.groupby('state')['count'].idxmax()]
    location_result = location_result.reset_index(drop=True)

    streamlit.header("Voters' Locations")
    paginate_table(location_result)

    # Check if all voters have voted
    if not election_ended and check_voting_completion():
        election_ended = True
        streamlit.success("Election has ended.")


streamlit.title("RealTime Election Voting DashBoard")
topic_name = "aggregated_votes_per_candidate"

sidebar()
data_updation()
