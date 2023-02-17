#############################################################
Prototyping new Butler, Registry, and Datastore relationships
#############################################################

This directory contains valid - but not executable - Python code intended to work through a different concept of how Butler, Registry, and Datastore might interact with each other.
Eventually code highlights, context, and a summary will be included in the technote itself, but for now this code stands more-or-less alone and should be considered more authoritative than the technote content, some of which it obsoletes.

A quick overview of goals and how things are going:

- This is in many ways a maximal-change proposal; I am trying to think about how Butler, Registry, and Datastore should have interacted from the beginning if I had known then what I know how.
  This includes a few adopted-but-not-implemented RFCs and some feature-request tickets we haven't gotten to yet that have been on my mind.
  I do not expect *everything* here to *ever* make it into the codebase, but I like to prototype without feeling too constrained by what we might break, and then pare things back once a complete picture is there.
  (When I started this, Tim said, "don't go overboard", so naturally...)

- New classes here are often replacements for classes of the same name in `lsst.daf.butler`.
  In other cases I just import existing classes from `lsst.daf.butler` because I'm not proposing to change them (or change them very little).

- There are precious few docstrings here, and there won't ever be many more.
  Instead, I've added a lot of type aliases (in ``aliases.py``) for the various things that are actually represented by `str`, `typing.Any`, or a few other core types, so big nested collections are easy to comprehend.
  I'm not saying we should use those aliases (let alone `typing.NewType`) in the real codebase (though it might be worth discussing).

- There are lot of dataclasses here that should be pydantic models instead; I'm using dataclasses because I don't want my relative lack of familiarity with pydantic to slow me down.

- We're moving all public Registry interfaces to Butler (usually with some modifications).

- The full Butler will remain a general-purpose concrete class, with SQL vs. https differences handled by a Registry implementation, and access to access-controlled Rubin object stores handled by a Datastore implementation.

- LimitedButler still exists as an ABC for both the full Butler and QuantumBackedButler.
  Other implementations may exist in the future (that could be useful for Prompt Processing, for example).

- I've added vectorized get_many/put_many/etc variants to LimitedButler because I think they might be useful (thinking about http latencies) and I wanted to work out the components relationships with that in mind.

- I probably have not thought as much as I should about async here.
  Maybe that could be another ticket on this technote after this one is merged.

- As introduced in the technote draft, I'm making Registry responsible for directly holding Datastore records, with Butler responsible for passing them between the two as needed.

- Again as introduced in the technote, I'm envisioning using journal files to track datasets that are Datastore-only, providing better consistency guarantees and avoiding long registry transactions.

- I'm prototyping everything in snake_case.
  I think that's worth actually changing whenever we're fundamentally breaking old code anyway (e.g. by moving methods from Registry to Butler), but in other places it's I'm just using snake_case here in order to not have to think about what exactly should and shouldn't be changed (for now).
