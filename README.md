### lazydb

Attempt to write sqlite like db in C from scratch learning purpose.

## Compile

compile by running
    `make`

run 
    `./lazydb`


### following

`    https://cstack.github.io/db_tutorial/`

-----

Phase 1 :
   

    insert into in memory, test table.
    following test scenario
    $./lazydb
    lazydb > insert 1 nishanth nishanth@test.com
    Executed.
    lazydb > insert 2 nish nishanthsp@git
    Executed.
    lazydb > select
    (1, nishanth, nishanth@test.com)
    (2, nish, nishanthsp@git)
    Executed.

----------------------

Phase 2 :


    test and debug.
    fixed buffer overflow on insertion.
    negative id check
    string size check


-----


    $./lazydb
    lazydb > insert 1 nishanth nishanth@test.com
    Executed.
    lazydb > insert -9 name mailid@mail
    id value cannote be negative.
    lazydb > insert 1 nishanthspshettynametoolongwhateversomelongnamehuh huhuhu
    column too long.   


