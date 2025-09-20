"""
project_producer_gillespie.py

Generate & update a JSON file with some fake biometric data for live plotting using Matplotlib.

Example JSON messages 
{"heart_rate": 72}
{"steps": 32}
{"caloires": 525, "carbs": 45, "protein": 20, "fat": 10}
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import json
import os
import random
import time
import pathlib

# Import external packages (must be installed in .venv first)
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("BUZZ_INTERVAL_SECONDS", 1))
    logger.info(f"Message interval: {interval} seconds")
    return interval

#####################################
# Set up Paths - write to a file the consumer will monitor
#####################################

# The parent directory of this file is its folder.
# Go up one more parent level to get the project root.
PROJECT_ROOT = pathlib.Path(__file__).parent.parent
logger.info(f"Project root: {PROJECT_ROOT}")

# Set directory where data is stored
DATA_FOLDER: pathlib.Path = PROJECT_ROOT.joinpath("data")
logger.info(f"Data folder: {DATA_FOLDER}")

# Set the name of the data file
DATA_FILE: pathlib.Path = DATA_FOLDER.joinpath("biometrics_live.json")
logger.info(f"Data file: {DATA_FILE}")

#####################################
# Define global variables
#####################################

# Define some lists for generating buzz messages
UPDATE_TYPE: list = ["heart_rate", "steps", "diet", "exercise"]

#####################################
# Define a function to generate buzz messages
#####################################


def generate_messages():
    """
    Generate a stream of fake biometric data in the JSON format
    Example JSON message
        {"heart_rate": 74}

    This function uses a generator, which yields one buzz at a time.
    Generators are memory-efficient because they produce items on the fly
    rather than creating a full list in memory.

    Because this function uses a while True loop, it will run continuously 
    until we close the window or hit CTRL c (CMD c on Mac/Linux).
    """
    while True:
        this_update_type = random.choice(UPDATE_TYPE)

        if this_update_type == "heart_rate":
            heart_rate = random.randint(60, 100)
            json_message = {
                "heart_rate": heart_rate
            }
        elif this_update_type == "steps":
            steps = random.randint(0, 100)
            json_message = {
                "steps": steps
            }
        elif this_update_type == "diet":
            calories = random.randint(200, 800)
            carbs = random.randint(20, 100)
            protein = random.randint(10, 50)
            fat = random.randint(5, 30)
            json_message = {
                "calories": calories,
                "carbs": carbs,
                "protein": protein,
                "fat": fat
            }
        else:
            duration = random.randint(10, 60)  # in minutes
            distance = round(random.uniform(1.0, 10.0), 2)  # in miles
            json_message = {
                "exercise_duration": duration,
                "exercise_distance": distance
            } 

        json_message["type"] = this_update_type # type: ignore

        # Yield the dictionary to the caller
        yield json_message


#####################################
# Define main() function to run this producer.
#####################################


def main() -> None:
    """
    Main entry point for this producer.

    It doesn't need any outside information, so the parentheses are empty.
    It doesn't return anything, so we say the return type is None.   
    The colon at the end of the function signature is required.
    All statements inside the function must be consistently indented. 
    This is a multiline docstring - a special type of comment 
    that explains what the function does.
    """

    logger.info("START producer...")
    logger.info("Hit CTRL c (or CMD c) to close.")
    
    # Call the function we defined above to get the message interval
    # Assign the return value to a variable called interval_secs
    interval_secs: int = get_message_interval()

    try:
        for message in generate_messages():
            logger.info(message)
            with DATA_FILE.open("a") as f:
                f.write(json.dumps(message) + "\n")
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info("Producer shutting down.")




#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
