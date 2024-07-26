package io.polaris.core.context;

/**
 * Represents the elements of a REST request associated with routing to independent and isolated
 * "universes". This may include properties such as region, deployment environment (e.g. dev, qa,
 * prod), and/or account.
 */
public interface RealmContext {
  String getRealmIdentifier();
}
