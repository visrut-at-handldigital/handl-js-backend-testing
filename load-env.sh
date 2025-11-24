#!/bin/bash

# Check if the .env file exists
if [ -f .env ]; then
    # Initialize an empty variable to hold the key and value
    key=""
    value=""
    
    while IFS= read -r line || [ -n "$line" ]; do
        # Skip empty lines and lines that start with #
        if [[ -z "$line" || "$line" =~ ^# ]]; then
            continue
        fi
        
        # If the line contains an '=', it might be a new key-value pair
        if [[ "$line" == *'='* ]]; then
            # If there's already a key being processed, export it
            if [ -n "$key" ]; then
                export "$key=$value"
            fi
            # Reset key and value
            key=$(echo "$line" | cut -d '=' -f 1 | xargs)
            value=$(echo "$line" | cut -d '=' -f 2- | xargs)
        else
            # If the line doesn't contain '=', it might be a continuation of the value
            value="$value\n$(echo "$line" | xargs)"
        fi
    done < .env

    # Export the last key-value pair if it exists
    if [ -n "$key" ]; then
        export "$key=$value"
    fi
fi