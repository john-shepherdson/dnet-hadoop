

## Checkout a specific release of the BIP-Ranker git repository

* Edit the `scmVersion` of the maven-scm-plugin in the pom.xml to point to the tag/release version you want to check out.

* Then perform the checkout with:

```
mvn scm:checkout
```

* The code should be visible under `src/main/bip-ranker` folder.