#############################################################
Prototyping new Butler, Registry, and Datastore relationships
#############################################################

This directory contains valid - but not executable - Python code intended to work through a different concept of how Butler, Registry, and Datastore might interact with each other.
Eventually code highlights, context, and a summary will be included in the technote itself, but for now this code stands more-or-less alone and should be considered more authoritative than the technote content, some of which it obsoletes.

A quick overview of conventions and caveats here:

- This is in many ways a maximal-change proposal; I am trying to think about how Butler, Registry, and Datastore should have interacted from the beginning if I had known then what I know how.
  This includes a few adopted-but-not-implemented RFCs and some feature-request tickets we haven't gotten to yet that have been on my mind.
  I do not expect *everything* here to *ever* make it into the codebase, but I like to prototype without feeling too constrained by what we might break, and then pare things back once a complete picture is there.
  (When I started this, Tim said, "don't go overboard", so naturally...)

- New classes here are often replacements for classes of the same name in `lsst.daf.butler`.
  In other cases I just import existing classes from `lsst.daf.butler` because I'm not proposing to change them (or change them very little).

- I've added a lot of type aliases (in ``aliases.py``) for the various things that are actually represented by `str`, `typing.Any`, so big nested collections are easy to comprehend.
  I'm not saying we should use those aliases (let alone `typing.NewType`) in the real codebase (though it might be worth discussing).

- There are lot of dataclasses here that should be pydantic models instead; I'm using dataclasses because I don't want my relative lack of familiarity with pydantic to slow me down.

- I have not tried to add docstrings that would be mostly duplicative of docs in the current codebase (i.e. methods that have not changed much).

- I have not tried to add docstrings that would be mostly duplicative of other docs in the prototype.
  In particular, base class docstrings aften mention derived class modifications, so derived class docstrings don't need to exist.
  I have duplicated some parameter docs where it's important for the interpretation of the arguments to be very precise to make the Butler/Registry/Datastore boundary work

- The docstrings I have added skew more towards explaining the design and intent than real ones should.

- The method implementations I have (often partially) implemented should be
considered pseudocode used to work out whether interfaces are compatible and consistency guarantees can be met, not production code.

- I probably have not thought as much as I should about async here.
  Maybe that could be another ticket on this technote after this one is merged.

- I have not tried to include caching in this prototype.
  Registry currently caches dataset types, collections, and some dimension records, but all within the manager objects.
  We probably want to move all of that into dedicated cache classes that can be held by Butler itself.
  While we could let Registry implementations do the caching, we want it to be client-side in both cases and that implies it'd be duplicative to put it in Registry.
  We also need any server-side caching that we keep to automatically retry on misses, because (unlike butler clients) it is not reasonable to assume that one butler server rarely cares about things modified in parallel by other servers, and can manaully refresh on accordingly.

- I'm prototyping everything in snake_case.
  I think that's worth actually changing whenever we're fundamentally breaking old code anyway (e.g. by moving methods from `Registry` to `Butler`), but in other places it's I'm just using snake_case here in order to have consistency within the prototype itself and improve readability there.
