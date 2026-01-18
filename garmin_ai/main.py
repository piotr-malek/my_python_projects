from utils import combine_garmin_data, parse_arguments
from training_data import get_activities, get_sleep_data, get_health_data, get_training_data, get_training_plan
from llm_interaction import send_prompt_to_gemini
import os
import sys

def main():
    args = parse_arguments()
    
    if not os.path.exists(args.prompt_file):
        print(f"Error: Prompt file '{args.prompt_file}' does not exist.")
        sys.exit(1)
        
    try:
        garmin_data = combine_garmin_data(
            get_activities(14),
            get_sleep_data(7),
            get_health_data(7),
            get_training_data(7)
        )
        training_plan = get_training_plan(7, 7)

        with open(args.prompt_file, 'r') as f:
            prompt_template = f.read()

        formatted_prompt = prompt_template.format(
            garmin_data=garmin_data,
            training_plan=training_plan
        )

        llm_response = send_prompt_to_gemini('gemini-1.5-flash', formatted_prompt)
        print(llm_response)

    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()