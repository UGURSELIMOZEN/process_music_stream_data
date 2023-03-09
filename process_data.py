import pandas as pd
import time
import os

def read_input_file(file_name):
    """Reads input file from path.
    Args:
        file_name: string: input file name
    Returns:
        music_stream_data: Dataframe: read dataframe
    """
    music_stream_data = pd.read_csv(filepath_or_buffer = os.getcwd() + "/" + file_name , sep="\t")
    return music_stream_data


def filter_desired_date_data(music_stream_data):
    """Filters dataframe for desired date.
    Args:
        music_stream_data: Dataframe: read dataframe
    Returns:
        desired_date_data: Dataframe: filtered dataframe
    """
    desired_date_data = music_stream_data[music_stream_data.PLAY_TS.str.contains("10/08/2016")]
    return desired_date_data


def get_distinct_play_count_by_client_data(desired_date_data):
    """Aggregates distinct play counts by client.
    Args:
        desired_date_data: Dataframe: filtered dataframe
    Returns:
        distinct_play_count_by_client_data: Dataframe: aggregated dataframe 
    """
    distinct_play_count_by_client_data = desired_date_data.groupby("CLIENT_ID").agg({"SONG_ID": "nunique"}) \
                                        .reset_index().rename(columns = {"SONG_ID": "DISTINCT_PLAY_COUNT"})
    return distinct_play_count_by_client_data


def get_distinct_play_count_per_user_data(distinct_play_count_by_client_data):
    """Aggregates distinct play counts per user.
    Args:
        distinct_play_count_by_client_data: Dataframe: aggregated dataframe
    Returns:
        distinct_play_count_per_user_data: Dataframe: result dataframe
    """
    distinct_play_count_per_user_data = distinct_play_count_by_client_data.groupby("DISTINCT_PLAY_COUNT").agg({"CLIENT_ID": "count"}) \
                                    .reset_index().rename(columns = {"CLIENT_ID": "CLIENT_COUNT"})
    return distinct_play_count_per_user_data


def insert_output_to_file(distinct_play_count_per_user_data):
    """Inserts result dataframe into csv file.
    Args:
        distinct_play_count_per_user_data: Dataframe: result dataframe
    Returns: None
    """
    distinct_play_count_per_user_data.to_csv("result.csv", index=False)


if __name__ == "__main__":
    start_time = time.time()
    music_stream_data = read_input_file("exhibitA-input.csv")
    desired_date_data = filter_desired_date_data(music_stream_data)
    distinct_play_count_by_client_data = get_distinct_play_count_by_client_data(desired_date_data)
    distinct_play_count_per_user_data = get_distinct_play_count_per_user_data(distinct_play_count_by_client_data)
    insert_output_to_file(distinct_play_count_per_user_data)
    print('Execution time', round(time.time()-start_time, 2), 'seconds.')
