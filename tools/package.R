library(devtools);

devtools::check()
devtools::spell_check()

# devtools::document(roclets=c('collate','namespace','rd'))

devtools::build()

#devtools::release()