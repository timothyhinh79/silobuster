This is a non-standard story meant to document the development of thought around an architectural need: logging the outputs of cleaning and quality assessment steps.

In a call between Skyler Young, Peter Bonnel and Eric Hummel, a first draft pseudo-schema in JSON was developed:

```json
{
    "id": "string",
    "source_data_set": "string",
    "link_entity": "string",
    "link_field": "string",
    "link_id": "string",
    "import_run_id": "string",
    "action_name": "string",
    "message": "string",
    "message_code?": "string",
    "prompt": [
      "string"
    ],
    "passed": "boolean"
}
```

This is good for logging informationg around a single field, but what about instances where we are cleaning/assessing concepts involving multiple fields?

For example, addresses in HSDS have each segment of the address stored in separate fields. Any attempt to log information about an address would involve aggregating info about multiple fields. We need a schema to accomdoate that.

Here's a first attempt at defining something along those lines. It enhances just the ability to document multiple fields.

```json
{
    "id": "string",
    "source_data_set": "string",
    "link_entity": "string",
    // "link_fields": [
    //     "string"
    // ],
    "link_id": "string",
    "import_run_id": "string",
    "action_name": "string",
    "message": "string",
    // "message_code?": "string",
    "prompts": [
      {
        "description": "string",
        "suggested_value": "string",
        "link_field": "string"
      }
    ],
    "passed": "boolean"
}
```

There is some duplication between the root level `link_fields` and the `fields` array inside `prompts`. Perhaps we only need the latter.

### questions:
- Is it enough to just suggest a _value_ for fields, or do we also need to give notes/descriptions for the fields too, like lables or something.
- What are cases where a promt doesn't even include suggesting a value.