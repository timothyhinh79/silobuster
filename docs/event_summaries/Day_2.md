# Summarize Day Two for posterity.
The primary developers on hand today were:
- Skyler Young
- Jamey Charris
- Ross Rogers

With additional stakholder and administor support from:
- Eric Hummel
- Kylie Johnson

Chris lended moral support all day.

We started at 8am. Most folks were wrapped up and going home by 5:30.

## Celebrate successes.
Jamey and Ross were heros.
- Ross wrote an entire Airflow module that cleans up contact info (phone, email, URL) in one day.
- Jamey significantly extended the training package for ML, and generated results models. Amazing.

## Lessons learned.
- Training the models will get best results of Data Managers help train them. Some duplicates are hard to spot without localized or specialized knowledge.
- Training ML models is best implemented as an interative process that starts with "easy" aspect, like addresses and organizations, and then uses success with that data as a signal to help refine further iterations with additional data (like adding phones, or even services into the mix).
- Ultimately, deduping should probably be run on a "constellation" of denormalized datapoints from all core tables (organization, service, and locations) and their associations.

## Summary of event.
This was an amazing and inspring weekend. We were served a curve ball immediately on Saturday morning when we realized we wouldn't be using the Dedupe service, and instead embarked on training our own models. Despite that, we saw success in six key areas:
1. We created an open source training implementation of Dedupe.io and started training our own models.
2. Wrote a "data cleaning" module for contact data, ran it in Airflow, and successfully cleaned up the data with it. This demonstrated the orchestration "step-by-step" model is viable.
3. Got _three_ data sets _mostly_ transformed and combined using DBT. This is definitely a great tool, although a full implementation needs a little more time for three entire data sets.
4. Started documenting the results of _phenominal_ thought leadership defining what constitutes "quality data".
5. Started documenting the necessary data to clean and store for metadata, suitable for generating reports.
6. Tested Placekey.io as a free tool for making addresses easier to deduplicate.

This was an unmitigated success, and has laid the groundwork for further work that is already being planned.

I have so much gratitude for everyone who attended and made this possible.