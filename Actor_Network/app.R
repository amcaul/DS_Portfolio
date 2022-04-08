library(shiny)
library(shinythemes)
library(data.table)
library(dplyr)
library(stringi)
library(stringr)
library(ggplot2)
library("aws.s3")
library(countrycode)
library(wordcloud2)
library(tidyverse)
library(arules)
library(networkD3)
library(igraph)
library(DT)



#edges = s3read_using(FUN = readRDS, bucket = "cdwbucket/AubreyTestData", object = "combinedEdges.RDS") %>% ungroup()
edges = readRDS("Event_Edges.RDS")

Encoding(edges$actor1) = "latin1"
edges$actor1<-str_to_title(iconv(edges$actor1, "latin1", "UTF-8"))
Encoding(edges$actor2) = "latin1"
edges$actor2<-str_to_title(iconv(edges$actor2, "latin1", "UTF-8"))

countrynames = unique(edges$country)
nodes = as.data.frame(table(c(edges$actor1, edges$actor2))) %>% filter(Freq >=100)
nodes = subset(nodes, !grepl('(', Var1, fixed = TRUE))
colnames(nodes) = c("actors", "Freq")

# Define UI for application that draws a histogram
ui <- 
  navbarPage("Global Events Application: Actors and Events Networks", collapsible = TRUE, inverse = TRUE, theme = shinytheme("flatly"),
             
             tabPanel("Macro-Level",
                      titlePanel("Actors on the Global Stage"),
                      sidebarLayout(
                        sidebarPanel(width = 3,
                                     h2("Filter by:"),
                                     radioButtons(
                                       "filter",
                                       label = "Choice",
                                       choices = list("Country" = 1, "Event Category" = 2, "Both" = 3), selected = 1),
                                     selectizeInput(
                                       "countries",
                                       label = "Country of Interest",
                                       choices = unique(countrynames),
                                       selected = "Belgium",
                                       multiple = FALSE
                                     ),
                                     selectizeInput(
                                       "event_type",
                                       label = "What Category of Event?",
                                       choices = c("Other", "Government Repression", "Conventional Violence", 
                                                   "Terrorist Attack", "Riot", "Protest"),
                                       selected = NULL,
                                       multiple = FALSE
                                     ),
                                     h2("Top 100 Actors (Filtered)"),
                                     wordcloud2Output("s", width = "500px", height = "500px"),
                                     h2("Global Actors Network (Filtered)"),
                                     downloadButton('downloadData', 'Download csv')
                        ),
                        
                        mainPanel(width = 9,
                                  h2("Force Network for Actors (Filtered)"),
                                  h4("Scroll to Zoom. Click and drag Nodes to Explore the Network"),
                                  textOutput("piping"),
                                  forceNetworkOutput(outputId = "force", width = "1600px", height = "1000px")),
                      
             )),
             
             tabPanel("Entity-Specific",
                      titlePanel("Events Network for Specific Actor"),
                      sidebarLayout(
                        sidebarPanel(width = 3,
                                     selectizeInput("act1", 
                                                    label = "Actor of Interest",
                                                    choices = unique(nodes$actors),
                                                    selected = "Kidnapper"),
                                     radioButtons(
                                       "hops",
                                       label = "Number of Hops",
                                       choices = list("One" = 1, "Two" = 2), selected = 1),
                                     h5("Two Hop Network is filtered to higher frequencies."),
                                     selectizeInput(
                                       "event_type2",
                                       label = "What Category of Event?",
                                       choices = unique(edges$category),
                                       selected = "Conventional Violence",
                                       multiple = TRUE
                                     ),
                                     h2("Actor-based Network Data"),
                                     downloadButton('downloadData1', 'Download csv')
                        ),
                        mainPanel(width = 9,
                                  h2("Force Network for Actors by Event Category"),
                                  h4("Scroll to Zoom. Click and drag Nodes to Explore the Network"),
                                  forceNetworkOutput(outputId = "targetedNetwork", width = "1600px", height = "1000px")),
                      )
             ),
  )

# Define server logic required to draw a histogram
server <- function(input, output) {
  output$piping = renderText({
    if (input$filter == 1) {
      print("Filtered by Country")
    }
    else if (input$filter == 2) {
      print("Filtered by Event Type")
    }
    else{
      print("Filtered by Both")
    }
  })
  output$s = renderWordcloud2({
    if (input$filter == 1) {
      actors = c(edges$actor1[edges$country == input$countries], edges$actor2[edges$country == input$countries])
      actors = as.data.frame(table(actors)) %>% filter(rank(desc(Freq))<=100)
      wordcloud2(actors)
    }
  else if (input$filter == 2) {
    actors = c(edges$actor1[edges$category == input$event_type], edges$actor2[edges$category == input$event_type])
    actors = as.data.frame(table(actors)) %>% filter(rank(desc(Freq))<=100)
    wordcloud2(actors)
  }
  else{
    actors = c(edges$actor1[edges$country == input$countries & edges$category == input$event_type], 
                      edges$actor2[edges$country == input$countries & edges$category == input$event_type])
    actors = as.data.frame(table(actors)) %>% filter(rank(desc(Freq))<=100)
    wordcloud2(actors)
  }
})
  output$force = renderForceNetwork({
    if (input$filter == 1) {
      pairings <- edges[edges$country == input$countries,] %>%  
        group_by(actor1, actor2, category) %>%
        summarise(weight = sum(weight)) %>% 
        ungroup()
      
      pairings = pairings[pairings$actor1 != pairings$actor2,]
      
      pairings = pairings %>% na.omit %>% top_n(500, weight)
      
      sources = ungroup(pairings) %>% 
        distinct(actor1) %>%
        rename(label = actor1)
      
      destination = ungroup(pairings) %>%
        distinct(actor2) %>%
        rename(label = actor2)
      
      nodes = full_join(sources, destination, by = "label")
      nodes <- nodes %>% rowid_to_column("id")
      
      edges <- pairings %>% 
        left_join(nodes, by = c("actor1" = "label")) %>% 
        rename(from = id)
      
      edges <- edges %>% 
        left_join(nodes, by = c("actor2" = "label")) %>% 
        rename(to = id)
      
      edges2 <- select(edges, from, to, weight, category)
      
      nodes2 = nodes[nodes$id %in% unique(c(edges2$from, edges2$to)),]
      g = graph_from_data_frame(edges2, directed = TRUE, vertices = nodes2)
      wc <- cluster_walktrap(g, weights = edges2$weight, steps = 6)
      members <- membership(wc)
      actor_d3 <- igraph_to_networkD3(g, group = members)
      actor_d3$nodes$names = nodes2$label
      actor_d3$links$weight = 1+(sqrt(edges2$weight))
      
      forceNetwork(Links = actor_d3$links, Nodes = actor_d3$nodes,
                   Source = 'source', Target = 'target', Value = 'weight', NodeID = 'names', Group = 'group', 
                   arrows = TRUE, opacity = 0.8, zoom = TRUE, bounded = TRUE, charge = -100, fontSize = 25, colourScale = JS("d3.scaleOrdinal(d3.schemeCategory10);"))
      
    }
    else if (input$filter == 2) {
      pairings <- edges[edges$category == input$event_type,] %>%  
        group_by(actor1, actor2, category) %>%
        summarise(weight = sum(weight)) %>% 
        ungroup()
      
      pairings = pairings[pairings$actor1 != pairings$actor2,]
      
      pairings = pairings %>% na.omit %>% top_n(500, weight)
      
      sources = ungroup(pairings) %>% 
        distinct(actor1) %>%
        rename(label = actor1)
      
      destination = ungroup(pairings) %>%
        distinct(actor2) %>%
        rename(label = actor2)
      
      nodes = full_join(sources, destination, by = "label")
      nodes <- nodes %>% rowid_to_column("id")
      
      edges <- pairings %>% 
        left_join(nodes, by = c("actor1" = "label")) %>% 
        rename(from = id)
      
      edges <- edges %>% 
        left_join(nodes, by = c("actor2" = "label")) %>% 
        rename(to = id)
      
      edges2 <- select(edges, from, to, weight, category)
      
      nodes2 = nodes[nodes$id %in% unique(c(edges2$from, edges2$to)),]
      
      g = graph_from_data_frame(edges2, directed = TRUE, vertices = nodes2)
      wc <- cluster_walktrap(g, weights = edges2$weight, steps = 6)
      members <- membership(wc)
      actor_d3 <- igraph_to_networkD3(g, group = members)
      actor_d3$nodes$names = nodes2$label
      actor_d3$links$weight = 1+(sqrt(edges2$weight))
      
      forceNetwork(Links = actor_d3$links, Nodes = actor_d3$nodes,
                   Source = 'source', Target = 'target', Value = 'weight', NodeID = 'names', Group = 'group',  
                   arrows = TRUE, opacity = 0.8, zoom = TRUE, bounded = TRUE, charge = -100, fontSize = 25, colourScale = JS("d3.scaleOrdinal(d3.schemeCategory10);"))
    }
    else{
      pairings <- edges[edges$country == input$countries & edges$category == input$event_type,] %>%  
        group_by(actor1, actor2, category) %>%
        summarise(weight = sum(weight)) %>% 
        ungroup()
      
      pairings = pairings[pairings$actor1 != pairings$actor2,]
      
      pairings = pairings %>% na.omit %>% top_n(500, weight)
      
      sources = ungroup(pairings) %>% 
        distinct(actor1) %>%
        rename(label = actor1)
      
      destination = ungroup(pairings) %>%
        distinct(actor2) %>%
        rename(label = actor2)
      
      nodes = full_join(sources, destination, by = "label")
      nodes <- nodes %>% rowid_to_column("id")
      
      edges <- pairings %>% 
        left_join(nodes, by = c("actor1" = "label")) %>% 
        rename(from = id)
      
      edges <- edges %>% 
        left_join(nodes, by = c("actor2" = "label")) %>% 
        rename(to = id)
      
      edges2 <- select(edges, from, to, weight, category)
      
      nodes2 = nodes[nodes$id %in% unique(c(edges2$from, edges2$to)),]
      g = graph_from_data_frame(edges2, directed = TRUE, vertices = nodes2)
      wc <- cluster_walktrap(g, weights = edges2$weight, steps = 6)
      members <- membership(wc)
      actor_d3 <- igraph_to_networkD3(g, group = members)
      actor_d3$nodes$names = nodes2$label
      actor_d3$links$weight = 1+(sqrt(edges2$weight))
      
      forceNetwork(Links = actor_d3$links, Nodes = actor_d3$nodes,
                   Source = 'source', Target = 'target', Value = 'weight', NodeID = 'names', Group = 'group', 
                   arrows = TRUE, opacity = 0.8, zoom = TRUE, bounded = TRUE, charge = -100, fontSize = 25, colourScale = JS("d3.scaleOrdinal(d3.schemeCategory10);"))
      }
  })
  output$downloadData <- downloadHandler(
    
    filename = function() { 
      paste("Topical_Events_Network_", Sys.Date(), ".csv", sep="")
    },
    content = function(file) {
      if (input$filter == 1) {
        pairings <- edges[edges$country == input$countries,] %>%  
          group_by(actor1, actor2, category) %>%
          summarise(weight = n()) %>% 
          ungroup()
      }
      else if (input$filter == 2) {
        pairings <- edges[edges$category == input$event_type,] %>%  
          group_by(actor1, actor2, country) %>%
          summarise(weight = n()) %>% 
          ungroup()
      }
      else{
        print("Filtered by Both")
      }
      pairings <- edges[edges$category == input$event_type & edges$country == input$countries,] %>%  
        group_by(actor1, actor2) %>%
        summarise(weight = n()) %>% 
        ungroup()
      
      pairings = pairings[pairings$actor1 != pairings$actor2,]
      
      pairings = pairings %>% na.omit %>% top_n(500, weight)
      write.csv(pairings, file)
    })
  
  output$targetedNetwork = renderForceNetwork({
    if (input$hops == 1) {
      edges1 = edges %>% filter(actor1 %like% input$act1 | actor2 %like% input$act1) %>% group_by (actor1, actor2, category) %>% summarize(weight = n())
      edges1 = edges1 %>% filter(category %in% input$event_type2)
      g = graph_from_data_frame(edges1, directed = TRUE, vertices = unique(c(edges1$actor1, edges1$actor2)))
      simpleG = simplify(
        g,
        remove.multiple = TRUE,
        remove.loops = TRUE
      )
      Isolated = which(degree(simpleG)==0)
      simpleG = delete.vertices(simpleG, Isolated)
      wc <- cluster_walktrap(simpleG)
      members <- membership(wc)
      actor_d3 <- igraph_to_networkD3(simpleG, group = members)
      forceNetwork(Links = actor_d3$links, Nodes = actor_d3$nodes,
                   Source = 'source', Target = 'target', Value = 'value', NodeID = 'name',
                   Group = 'group', 
                   arrows = TRUE, opacity = 0.8, zoom = TRUE, bounded = TRUE, charge = -100, fontSize = 15)
    }
    else{
      edges1 = edges %>% filter(actor1 %like% input$act1 | actor2 %like% input$act1) %>% group_by (actor1, actor2, category) %>% summarize(weight = n())
      edges1 = edges1 %>% filter(category %in% input$event_type2)
      full_list = unique(c(edges1$actor1, edges1$actor2))
      edges2 = edges %>% filter(actor1 %in% full_list | actor2 %in% full_list) %>% group_by (actor1, actor2, category) %>% summarize(weight = n()) %>% ungroup()
      edges2 = edges2 %>% filter(category %in% input$event_type2) %>% slice_max(weight, n = 500)
      g = graph_from_data_frame(edges2, directed = TRUE, vertices = unique(c(edges2$actor1, edges2$actor2))) 
      simpleG = simplify(
        g,
        remove.multiple = TRUE,
        remove.loops = TRUE
      )
      Isolated = which(degree(simpleG)==0)
      simpleG = delete.vertices(simpleG, Isolated)
      wc <- cluster_walktrap(simpleG)
      members <- membership(wc)
      actor_d3 <- igraph_to_networkD3(simpleG, group = members)
      forceNetwork(Links = actor_d3$links, Nodes = actor_d3$nodes,
                   Source = 'source', Target = 'target', Value = 'value', NodeID = 'name',
                   Group = 'group', 
                   arrows = TRUE, opacity = 0.8, zoom = TRUE, bounded = TRUE, charge = -100, fontSize = 15)
    }
  })
  
  output$downloadData1 <- downloadHandler(
    
    filename = function() { 
      paste("Actor_Based_Network_", Sys.Date(), ".csv", sep="")
    },
    content = function(file) {
      if (input$hops == 1) {
        pairings = edges %>% filter(actor1 %like% input$act1 | actor2 %like% input$act1) %>% group_by (actor1, actor2, category) %>% summarize(weight = n())
        pairings = pairings %>% filter(category %in% input$event_type2) %>% ungroup()
      }

      else{
        edges1 = edges %>% filter(actor1 %like% input$act1 | actor2 %like% input$act1) %>% group_by (actor1, actor2, category) %>% summarize(weight = n())
        edges1 = edges1 %>% filter(category %in% input$event_type2)
        full_list = unique(c(edges1$actor1, edges1$actor2))
        edges2 = edges %>% filter(actor1 %in% full_list | actor2 %in% full_list) %>% group_by (actor1, actor2, category) %>% summarize(weight = n()) %>% ungroup()
        pairings = edges2 %>% filter(category %in% input$event_type2) %>% slice_max(weight, n = 500) %>% ungroup()
      }
      
      pairings = pairings[pairings$actor1 != pairings$actor2,]
      
      pairings = pairings %>% na.omit %>% top_n(500, weight)
      write.csv(pairings, file)
    })
  
  
}

# Run the application 
shinyApp(ui = ui, server = server)
