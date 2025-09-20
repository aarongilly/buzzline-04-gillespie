"""
project_consumer_gillespie.py

Read a JSON-formatted file as it is being written. 

JSON messages are processed and a live-updating chart is created using Matplotlib.
The messages may be of several types, but they all contain a "type" field.
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import json
import os # for file operations
import sys # to exit early
import time
import pathlib
from collections import defaultdict  # data structure for counting author occurrences

# IMPORTANT
# Import Matplotlib.pyplot for live plotting
import matplotlib.pyplot as plt

# Import functions from local modules
from utils.utils_logger import logger


#####################################
# Set up Paths - read from the file the producer writes
#####################################

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DATA_FOLDER = PROJECT_ROOT.joinpath("data")
DATA_FILE = DATA_FOLDER.joinpath("biometrics_live.json")

logger.info(f"Project root: {PROJECT_ROOT}")
logger.info(f"Data folder: {DATA_FOLDER}")
logger.info(f"Data file: {DATA_FILE}")

#####################################
# Set up data structures
#####################################

author_counts = defaultdict(int)

#####################################
# Set up live visuals
#####################################

fig, ax = plt.subplots()
plt.ion()  # Turn on interactive mode for live updates

#####################################
# Define an update chart function for live plotting
# This will get called every time a new message is processed
#####################################


def update_chart():
    """Update the live chart with the latest author counts."""
    # Prepare data for each subplot
    # 1. Stacked bar chart for diet (assume messages have 'diet' dict: {food: count})
    # 2. Line chart for heart_rate (assume messages have 'heart_rate' field)
    # 3. Line chart for steps (assume messages have 'steps' field)
    # 4. Line chart for exercise duration & distance (assume messages have 'exercise': {'duration': x, 'distance': y})

    # We'll need to store historical data for line charts
    if not hasattr(update_chart, "history"):
        update_chart.history = {
            "heart_rate": [],
            "steps": [],
            "exercise_duration": [],
            "exercise_distance": [],
            "diet": defaultdict(lambda: defaultdict(int)),  # author -> food -> count
            "authors": [],
            "msg_count": 0,
        }

    history = update_chart.history

    # Update history with latest author_counts
    history["authors"] = list(author_counts.keys())
    history["msg_count"] += 1

    # For demonstration, assume the last processed message is available as a global
    # In real code, you would refactor to pass the latest message data to update_chart
    latest_msg = getattr(update_chart, "latest_msg", {})
    author = latest_msg.get("author", "unknown")

    # Update diet data
    if "diet" in latest_msg:
        for food, count in latest_msg["diet"].items():
            history["diet"][author][food] += count

    # Update heart_rate
    if "heart_rate" in latest_msg:
        history["heart_rate"].append(latest_msg["heart_rate"])
    else:
        history["heart_rate"].append(None)

    # Update steps
    if "steps" in latest_msg:
        history["steps"].append(latest_msg["steps"])
    else:
        history["steps"].append(None)

    # Update exercise
    if "exercise" in latest_msg:
        history["exercise_duration"].append(latest_msg["exercise"].get("duration", None))
        history["exercise_distance"].append(latest_msg["exercise"].get("distance", None))
    else:
        history["exercise_duration"].append(None)
        history["exercise_distance"].append(None)

    # Clear and set up 4 subplots
    fig.clf()
    axs = fig.subplots(2, 2)
    axs = axs.flatten()

    # 1. Stacked bar chart for diet
    ax = axs[0]
    diet_data = history["diet"]
    foods = set()
    for food_counts in diet_data.values():
        foods.update(food_counts.keys())
    foods = sorted(foods)
    authors = sorted(diet_data.keys())
    bottom = [0] * len(authors)
    for food in foods:
        values = [diet_data[author].get(food, 0) for author in authors]
        ax.bar(authors, values, bottom=bottom, label=food)
        bottom = [b + v for b, v in zip(bottom, values)]
    ax.set_title("Diet (stacked bar)")
    ax.set_ylabel("Count")
    ax.legend(fontsize="small")

    # 2. Line chart for heart_rate
    ax = axs[1]
    ax.plot(history["heart_rate"], marker="o", color="red")
    ax.set_title("Heart Rate")
    ax.set_ylabel("BPM")

    # 3. Line chart for steps
    ax = axs[2]
    ax.plot(history["steps"], marker="o", color="blue")
    ax.set_title("Steps")
    ax.set_ylabel("Steps")

    # 4. Line chart for exercise duration & distance
    ax = axs[3]
    ax.plot(history["exercise_duration"], label="Duration (min)", color="green")
    ax.plot(history["exercise_distance"], label="Distance (km)", color="purple")
    ax.set_title("Exercise")
    ax.set_ylabel("Value")
    ax.legend(fontsize="small")

    plt.tight_layout()
    plt.draw()
    plt.pause(0.01)


#####################################
# Process Message Function
#####################################


def process_message(message: str) -> None:
    """
    Process a single JSON message and update the chart.

    Args:
        message (str): The JSON message as a string.
    """
    
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        message_dict: dict = json.loads(message)
       
        # Ensure the processed JSON is logged for debugging
        logger.info(f"Processed JSON message: {message_dict}")

        # Ensure it's a dictionary before accessing fields
        if isinstance(message_dict, dict):
            # Extract the 'author' field from the Python dictionary
            author = message_dict.get("author", "unknown")
            logger.info(f"Message received from author: {author}")

            # Increment the count for the author
            author_counts[author] += 1

            # Log the updated counts
            logger.info(f"Updated author counts: {dict(author_counts)}")

            # Update the chart
            update_chart()

            # Log the updated chart
            logger.info(f"Chart updated successfully for message: {message}")

        else:
            logger.error(f"Expected a dictionary but got: {type(message_dict)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


#####################################
# Main Function
#####################################


def main() -> None:
    """
    Main entry point for the consumer.
    - Monitors a file for new messages and updates a live chart.
    """

    logger.info("START consumer.")

    # Verify the file we're monitoring exists if not, exit early
    if not DATA_FILE.exists():
        logger.error(f"Data file {DATA_FILE} does not exist. Exiting.")
        sys.exit(1)

    try:
        # Try to open the file and read from it
        with open(DATA_FILE, "r") as file:

            # Move the cursor to the end of the file
            file.seek(0, os.SEEK_END)
            print("Consumer is ready and waiting for new JSON messages...")

            while True:
                # Read the next line from the file
                line = file.readline()

                # If we strip whitespace from the line and it's not empty
                if line.strip():  
                    # Process this new message
                    process_message(line)
                else:
                    # otherwise, wait a half second before checking again
                    logger.debug("No new messages. Waiting...")
                    delay_secs = 0.5 
                    time.sleep(delay_secs) 
                    continue 

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        plt.ioff()
        plt.show()
        logger.info("Consumer closed.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
