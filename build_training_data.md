This file provides discoveries on how to mangle different types of data that we are training on in order to produce a high quality set of (messed up) training data.

1. Each type should be it's own method in it's own file to allow multiple devs to work on this part of the project without creating merge conflicts.
2. We have documented suggested percentages of mangling, but this should be an adjustable threshold.

## Name
Name fields for organizations (and later services).

As this data is not highly structured, nor does it even follow a consistent style guide between vendors, there is little to account for beyond typos or missing values.

**Mangle**
- Randomly remove 2% of characters.
- Randomly replace 2% with another random alpha character.
- Set approximately 2% of names to `null`.