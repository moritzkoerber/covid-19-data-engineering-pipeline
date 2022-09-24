import {
  Cluster,
  ClusterType,
  NodeType,
  Table,
} from "@aws-cdk/aws-redshift-alpha";
import { Construct } from "constructs";
import { RemovalPolicy, Stack, StackProps } from "aws-cdk-lib";
import { Vpc } from "aws-cdk-lib/aws-ec2";
import { Secret } from "aws-cdk-lib/aws-secretsmanager";
import { Role } from "aws-cdk-lib/aws-iam";

export class RedshiftStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const masterUserCredentials = Secret.fromSecretCompleteArn(
      this,
      "masterUserSecret",
      `arn:aws:secretsmanager:${this.region}:${this.account}:secret:redshift_admin-KPRsmn`
    );

    const redshiftServiceRole = Role.fromRoleName(
      this,
      "redshiftServiceRole",
      "RedshiftServiceRole"
    );

    const redshiftCluster = new Cluster(this, "RedshiftCluster", {
      clusterType: ClusterType.SINGLE_NODE,
      clusterName: "vaccinations-redshift-cluster",
      defaultDatabaseName: "rki",
      masterUser: {
        masterUsername: masterUserCredentials
          .secretValueFromJson("username")
          .toString(),
        masterPassword: masterUserCredentials.secretValueFromJson("password"),
      },
      vpc: Vpc.fromLookup(this, "DefaultVPC", { isDefault: true }),
      roles: [redshiftServiceRole],
      nodeType: NodeType.DC2_LARGE,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const table = new Table(this, "Table", {
      tableColumns: [
        { name: "date", dataType: "varchar" },
        { name: "administeredvaccinations", dataType: "BIGINT" },
        { name: "vaccinated", dataType: "BIGINT" },
        { name: "vaccination_biontech", dataType: "BIGINT" },
        { name: "vaccination_moderna", dataType: "BIGINT" },
        { name: "vaccination_astrazeneca", dataType: "BIGINT" },
        { name: "vaccination_janssen", dataType: "BIGINT" },
        { name: "vaccination_novavax", dataType: "BIGINT" },
      ],
      cluster: redshiftCluster,
      databaseName: "rki",
      tableName: "vaccinations",
      adminUser: masterUserCredentials,
    });
  }
}
