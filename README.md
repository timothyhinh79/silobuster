# Silobuster! documentation.

This is a repository of stories (AKA project definitions), code, and daily event summaries. Everything will be included: our successes, failures, and lessons learned.

This repository exists initially to facilitate an event, SiloBuster! 2022, but lives on for future contributors.

## Principles guiding this event:

1. Non-destructive data storing and editing at the record level (not the field level). This applies to duplicate records as well.
2. Use existing open source based cloud services to speed up development.
3. When it is absolutely expedient to do so, and there are no great open source options, ignore 2.
4. “Go now”. There should always be a default, canonical version of a record/set.
5. Document everything thoroughly. If it’s not documented, it never happened.
Questions are valuable outputs. Questions should be documented. I.e. What do we know we don’ t know?
6. Although database architecture is based on HSDS, we are modeling and storing complex relationships that go beyond the base specification. Extend the tables and fields where necessary, but if at all possible, _do not change the spec_.

## Etiquette.
Because this is a relatively small event, we are running everything out of a single Github repo. Please observe the following to help prevent merge conflicts etc:

1. Be clear about which code subdirectory you are working on, and only push code to that directory.
2. If you have a collaborator in the same directory, self organize and coordinate merges as you see fit.
3. Only event facilitors will update and push changes outside the code repository.

## Backstory and goals of this event.

Enterprise organizations like non-profit call centers, government agencies, and other CBOs are seeing the benefit of collaborating on shared resource directories. Mass duplication of data between systems is a waste for everyone.

In Washington State, three of the largest such organizations have come together as the Washington State Resource Data Collaborative. They seek open and transparent tools that will allow them to compare, share, and collaboratively manage their data.

There was a previous software solution, called ServiceNet, that could do this. It ultimately failed, but left us with invaluable lessons. We have full access to all of them. Read more about ServiceNet below.

The goal of this initiative is to carry on where ServiceNet left off, working closely with key stakeholders in Washington State

## Key lessons from ServiceNet.

1. It was a monolithic product instead of a collection of open tools. The lack of flexibility is what, in part, killed it. We are approaching this anew, using microservices architecture plus _existing_ processes, tools, and best practices that can be adapted to individual use cases.

2. The crux of collaborative data management is actually _content management_. That is, merging as much data as possible, while storing different versions of "soft" data like descriptions. Our architecture must take that management into account at the deepest levels.

3. Simply presenting stakeholders with thousands of potential duplicates creates _more_ work for them, and lessens the value of collaboration. The real value of this software would be identifing truth among data and help merge duplicates efficiently.

### A couple more technical lessons.

1. ServiceNet primarily utilized API integrations, which are a lot of work for stakeholders to integrate with on their end. While API integration will eventually be a part of this project, we are staring with bulk import orchestration using Airflow + connectors (Fivetran, Airbyte, etc). This is, again, very flexible, and gives participating members more flexibility to easily share data from different formats and using different methods.

2. ServiceNet is some very nifty code written primarily in Java, via the JHipster library. While good code, the talent pool of developers available to work on it has been small. We are adopting languages that are generally more approachable and widely used in open source commumities: Python, Javascript, and C#.

Testing ability to push. Remove later.