# Purpose

Powerful library for handling complex-datatypes in Hive utilizing macros

#Installation

Clone this repo and build it using gradle, feel free to adjust the hive version to match exactly the one you want it to.
Or download a release from the release version.
 
#Provided Functions

## Reduce Collection

The most powerful abstraction in this library. Compare to 
[Java's Collector](https://docs.oracle.com/javase/8/docs/api/java/util/stream/Collector.html) or 
[PHP's array_reduce()](http://docs.php.net/manual/en/function.array-reduce.php).


### signature

#### generics:

* `T` Return Type of the macro and the UDF. Will continuously be passed into the macro as parameter 2 or 3, so the macro can use this variable to remember intermediate state.
* `V` the value type of the collection.
* `K` Only present when the collection is a map type. Represents the key of the map.

#### parameters:

* `macroName` needs to be the string name of the macro, It needs to be a string literal and can't be a column references as it it's only resolved during compile time.
* `collection` needs to be either a map or an array.
* `initial value`  The initial value contains the type information T and is is (for now required to be non void). The provided UDF TypeFromString can be used to also pass the type information.
* `useInit` An option allows users to pass null to the first invocation, even when a not useful variable had to be passed into initial_value as a type hint.
* `varargs` additional parameters that can be carried through to the macro invocation.

```
T macro constmacroName ([key K], value V, variable T, varargs ....) 

T reduce_collection(String macroName, collection (MAP<K,V> | LIST<V>), initial_value T, boolean useInit, varargs ...)
```

### examples

Counting the occurrences of a very deeply nested field of type string. The result is a map from string => int containing the number of occurrences.
Execution in Hive will not involve a reduce phase as it would be with `Lateral View explode group by theStringField`.


```
create temporary macro updateWordCount(word string, result_map map<string,int>)
trv_udf.update_collection(result_map,word, if(result_map[word] is not null,result_map[word]+1,1));

create temporary macro reduce_inner_map(key_integer int, value_strings array<string>,result_map map<string,int>)
if(key_integer is not null,trv_udf.reduce_collection("updateWordCount",value_strings,result_map),result_map);

create temporary macro extract_deeper_field(inputstruct struct<deeper:map<int,array<string>>>,result_map map<string,int>)
trv_udf.reduce_collection("reduce_inner_map",inputstruct.deeper,result_map);

select 
trv_udf.reduce_collection("extract_deeper_field",
array(named_struct("deeper",map(4,array("a"),2,array("a","b"))),
    named_struct("deeper",map(24,array("a","b","c"),42,array("a","b","c","d"))),
    named_struct("deeper",map(24,array("a","b","c","d","e"),42,array("a","b","c","d","e","f")))),map("dummy",0),false);

=>

{"a":6,"b":5,"c":4,"d":3,"e":2,"f":1}

```

## MAP Collection

transform elements inside a map or array. If the lambda returns a struct with key and value fields, it will get mapped into a map

```
create temporary macro to_map(value string ) named_struct("key",substr(value,1,1),"value" , substr(value,2,1) * substr(value,2,1))

select map_collection("to_map",array("a2","b5","c7","d9")) 


=> 


{"a":4.0,"b":25.0,"c":49.0,"d":81.0}
```
If the collection is a map, key and value will be passed to the macro

```
create temporary macro from_map(`key` string, value int ) lpad("",value,`key`)

select map_collection("from_map",map("a",1,"b",4,"c",8))

=>


["a","bbbb","cccccccc"]
```

## Filter Collection

Allows to remove elements from a collection. The macro return a boolean. True will retain the record in the output, false will remove the record from the output. With maps no promise is made about the ordering.


### example

We filter an array so that only the elements are kept that have the square of the number inside the string greater than 80.

```
create temporary macro sqaured_numer_greater_80(value string ) substr(value,2,1)* substr(value,2,1) > 80 

select filter_collection("sqaured_numer_greater_80",array("a2","b5","c7","d9"))

=>

["d9"]
```



## Clear Collection

removes all the elements from a collection.

```
select clear_collection(array(42)) 

=> 

[]
```




