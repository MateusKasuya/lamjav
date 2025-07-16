"""
Check The Odds API Key and Credits.

This script checks which API key is being used and shows credit information.
"""

import sys
import os
from typing import Optional, Dict, Any

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from lib_dev.theoddsapi import TheOddsAPILib


def check_api_key():
    """
    Check which API key is being used and display credit information.
    """
    print("🔍 Checking The Odds API configuration...")

    # Check environment variable
    api_key = os.getenv("THEODDSAPI_API_KEY")
    if not api_key:
        print("❌ THEODDSAPI_API_KEY environment variable not found!")
        print("Please set the environment variable with your API key.")
        return

    # Show partial key for security
    if len(api_key) > 8:
        masked_key = api_key[:4] + "*" * (len(api_key) - 8) + api_key[-4:]
    else:
        masked_key = "*" * len(api_key)

    print(f"✅ API Key found: {masked_key}")
    print(f"📏 Key length: {len(api_key)} characters")

    # Test the API key with a simple request
    try:
        print("\n🧪 Testing API key with a simple request...")
        theoddsapi = TheOddsAPILib()

        # Make a simple request to check credits
        response = theoddsapi.get_sports(all_sports=False)

        if response is not None:
            print("✅ API key is valid and working!")
            print(f"📊 Found {len(response)} sports")

            # Check response headers for credit information
            print("\n📈 Credit Information:")
            print("Note: Credit information is shown in response headers")
            print("You can check your dashboard at: https://the-odds-api.com/dashboard")

        else:
            print("❌ API key test failed - no response received")

    except Exception as e:
        print(f"❌ Error testing API key: {e}")
        print("This could indicate:")
        print("- Invalid API key")
        print("- Network connectivity issues")
        print("- API service problems")


def check_environment_variables():
    """
    Check all relevant environment variables.
    """
    print("\n🔧 Environment Variables Check:")

    # Check for .env file
    env_file_exists = os.path.exists(".env")
    print(f"📁 .env file exists: {env_file_exists}")

    # Check common environment variable names
    possible_keys = [
        "THEODDSAPI_API_KEY",
        "THE_ODDS_API_KEY",
        "ODDS_API_KEY",
        "API_KEY",
    ]

    for key in possible_keys:
        value = os.getenv(key)
        if value:
            if len(value) > 8:
                masked_value = value[:4] + "*" * (len(value) - 8) + value[-4:]
            else:
                masked_value = "*" * len(value)
            print(f"✅ {key}: {masked_value}")
        else:
            print(f"❌ {key}: Not set")


def main():
    """
    Main function to check API configuration.
    """
    print("=" * 60)
    print("THE ODDS API KEY CHECKER")
    print("=" * 60)

    check_environment_variables()
    check_api_key()

    print("\n" + "=" * 60)
    print("💡 TIPS:")
    print("- Make sure your API key is valid and has sufficient credits")
    print("- Check your dashboard at: https://the-odds-api.com/dashboard")
    print("- Historical odds endpoints require a paid plan")
    print(
        "- Each historical event odds request costs 230 credits (23 markets × 10 credits)"
    )
    print("=" * 60)


if __name__ == "__main__":
    main()
