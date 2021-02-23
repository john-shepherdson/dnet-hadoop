<rel inferred="$inferred$" trust="$trust$" inferenceprovenance="$inferenceprovenance$" provenanceaction="$provenanceaction$">
$if(validated)$<validated date="$validationdate$"/>$else$$endif$
<to class="$class$" scheme="$scheme$" type="$type$">$objIdentifier$</to>
    $metadata:{ it | $it$ }$
</rel>