project(
     'arrow-partial-aggr'
    ,'cpp'
    ,version         : '0.1'
    ,default_options : ['warning_level=3', 'cpp_std=c++14']
)


# >> Dependencies
dep_arrow = dependency('arrow-dataset', version: '>=6.0.1', static: false)


# >> Executables
exe_test = executable('simple'
    ,'src/simple.cpp'
    ,'src/ops.cpp'
    ,install     : false
    ,dependencies: dep_arrow
)
