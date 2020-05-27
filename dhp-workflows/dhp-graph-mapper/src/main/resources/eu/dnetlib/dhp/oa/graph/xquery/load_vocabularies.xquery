for $x in collection(' /db/DRIVER/VocabularyDSResources/VocabularyDSResourceType') 
	let $vocid := $x//VOCABULARY_NAME/@code
	let $vocname := $x//VOCABULARY_NAME/text()
	for $term in ($x//TERM)
		return concat($vocid,' @=@ ',$vocname,' @=@ ',$term/@code,' @=@ ',$term/@english_name)
