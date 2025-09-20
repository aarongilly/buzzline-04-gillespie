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
    # 1. Line chart for heart_rate (assume messages have 'heart_rate' field)
    # 2. Line chart for steps (assume messages have 'steps' field)

    # We'll need to store historical data for line charts
    if not hasattr(update_chart, "history"):
        update_chart.history = {
            "heart_rate": [],
            "steps": [],
        }

    history = update_chart.history

    # Update history with latest author_counts
    history["authors"] = list(author_counts.keys())
    # history["msg_count"] += 1

    # For demonstration, assume the last processed message is available as a global
    # In real code, you would refactor to pass the latest message data to update_chart
    latest_msg = getattr(update_chart, "latest_msg", {})
    author = latest_msg.get("author", "unknown")

    # Update heart_rate
    if "heart_rate" in latest_msg:
        history["heart_rate"].append(latest_msg["heart_rate"])
    # else:
    #     history["heart_rate"].append(None)

    # Update steps
    if "steps" in latest_msg:
        history["steps"].append(latest_msg["steps"])
    # else:
    #     history["steps"].append(None)

    # Clear and set up subplots
    fig.clf()
    axs = fig.subplots(1, 2)
    axs = axs.flatten()

    # 1. Line chart for heart_rate
    ax = axs[0]
    ax.plot(history["heart_rate"], marker="o", color="red")
    ax.set_title("Heart Rate")
    ax.set_ylabel("BPM")

    # 3. Line chart for steps
    ax = axs[1]
    ax.plot(history["steps"], marker="o", color="blue")
    ax.set_title("Steps")
    ax.set_ylabel("Steps")

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
            # Extract the 'type' field from the Python dictionary
            message_type = message_dict.get("type", "unknown")
            logger.info(f"Message received of type: {message_type}")

            # Filter down to only relevant message types for demo
            if message_type != "heart_rate" and message_type != "steps":
                return

            # Store the latest message for chart updating
            update_chart.latest_msg = message_dict

            # Log the updated counts
            logger.info(f"Message sent to chart {dict(message_dict)}")

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
