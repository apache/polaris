### Upgrade notes

- NoSQL persistence: the format of page tokens returned by list operations changed. Page tokens
  issued by an earlier version are rejected with 400 Bad Request, so clients must restart a listing
  that was in progress during the upgrade. During a rolling upgrade, nodes that have not been
  upgraded yet also reject page tokens issued by upgraded nodes.
