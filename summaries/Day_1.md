# Summarize Day One for posterity.
The primary developers on hand today were:
- Skyler Young
- Jamey Charris
- Peter Bonnell

With additional stakholder and administor support from:
- Will Jenkins
- Eric Hummel
- Sarah Solomon
- Vesla Tonnessen

We started at 8am. Most folks were wrapped up and going home by 5:30. It's rumored that Skyler kept working on data transformation until 11pm.

## Celebrate successes.
The first day had the most participation and the highest energy. It was fun and engaging.

- Jamey made great progress starting a ML training package written in Python, and using Jupiter Notebook.
- Sarah, Vesla, Will, Eric, and Peter were all present from about 10AM to 12PM dicsussing how to define "data quality". Their thought leadership on this was very exciting.
- Skyler got enough data merged into the same tables to help faciliate training the Dedupe library, and pushed DBT to it's breaking point.
- Peter got the bulk of his work done testing Placekey.io.

## Lessons learned.
- Dedupe.io is kind of expensive to use as a service, and all of the developers recommended training our own models, and providing open source tools to help others do the same.
- DBT is not best suited for transforming _and merging_ large data sets; the latter should be a separate step orchestrated in Airflow. (We need to do something like that to assign UUIDs anyway).
- At a high level, Eric and Skyler discussed architecture and decided the core infrastructure for this system should be stream based (ie Kafka) instead of batch based, with microservices that can take batches and turn them into streams.
- The Data Quality workgroup tallied a number of useful findings in a Google Doc: https://docs.google.com/document/d/1j_gkPlw1beqmrxi1C17fr7CVJcNfKUzqBNChGv9XoGI/edit#

## What's next.
- We need to see where this ML training package goes, and whether we can get a working AirFlow step based on the generated model this weekend. That's looking aggresive, but we're hopeful.
- There is more specific work to be done on Data Quality, but the team needs to be supported with specific resources like having their data in a flatfile for review (which we do have, actually), and the HSDS spec handy for comparison.
- We're not clear yet if Placekey.io is helpful, so we'll followup on that.
- Finish getting all of the data transformed into HSDS 3.0 and put in the same DB (still with duplicates).
- We don't know yet precisely how or in what schema we're going to store workflow/state data.