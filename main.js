console.log("loaded");

const width = window.innerWidth;
const height = window.innerHeight;

const svg = d3
  .select("#main_canvas")
  .attr("viewBox", [0, 0, width, height])
  .attr("preserveAspectRatio", "xMidYMid meet");

d3.json("data/network_temp/temp.json").then((data) => {
  console.log("JSON Data:", data); // Log to verify data structure

  const linkScale = d3
    .scaleLinear()
    .domain(d3.extent(data.links, (d) => d.weight))
    .range([1, 5]);

  const opacityScale = d3
    .scaleLinear()
    .domain(d3.extent(data.links, (d) => d.weight))
    .range([0.3, 1]);

  const radiusScale = d3
    .scaleSqrt()
    .domain(d3.extent(data.nodes, (d) => d.occurrence))
    .range([5, 20]);

  const simulation = d3
    .forceSimulation(data.nodes)
    .force(
      "link",
      d3
        .forceLink(data.links)
        .id((d) => d.id)
        .distance(100),
    )
    .force("charge", d3.forceManyBody().strength(-200))
    .force("center", d3.forceCenter(width / 2, height / 2))
    .on("end", () => {
      simulation.stop();
    });

  // Create link group
  const linkGroup = svg.append("g").attr("stroke", "#999");

  // Create links with data binding
  const link = linkGroup
    .selectAll("line")
    .data(data.links)
    .join("line")
    .attr("stroke-width", (d) => linkScale(d.weight))
    .attr("stroke-opacity", (d) => opacityScale(d.weight));

  // Create node group
  const nodeGroup = svg
    .append("g")
    .attr("stroke", "#fff")
    .attr("stroke-width", 1.5);

  // Create nodes with data binding
  const node = nodeGroup
    .selectAll("circle")
    .data(data.nodes)
    .join("circle")
    .attr("r", (d) => radiusScale(d.occurrence))
    .attr("fill", "#69b3a2");

  // Add tooltips to nodes
  node.append("title").text((d) => d.name);

  // Simulation tick
  simulation.on("tick", () => {
    link
      .attr("x1", (d) => d.source.x)
      .attr("y1", (d) => d.source.y)
      .attr("x2", (d) => d.target.x)
      .attr("y2", (d) => d.target.y);

    node.attr("cx", (d) => d.x).attr("cy", (d) => d.y);
  });

  const tooltip = d3
    .select("body")
    .append("div")
    .attr("class", "tooltip")
    .style("position", "absolute")
    .style("background", "#fff")
    .style("padding", "5px")
    .style("border", "1px solid #ccc")
    .style("border-radius", "3px")
    .style("visibility", "hidden");

  node
    .on("mouseover", function (event, d) {
      tooltip.text(d.name);
      tooltip.style("visibility", "visible");
    })
    .on("mousemove", function (event) {
      tooltip
        .style("top", event.pageY - 10 + "px")
        .style("left", event.pageX + 10 + "px");
    })
    .on("mouseout", function () {
      tooltip.style("visibility", "hidden");
    });
});
