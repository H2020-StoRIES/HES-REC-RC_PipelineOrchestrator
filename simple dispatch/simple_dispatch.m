clc; clear; close all;

% Define time steps
T = 24;

% Define system capacities
P_HP_max = 450*1000;
E_HP_max = 2575.125*1000;

P_BAT_max = 6806*1000;
E_BAT_max = 6806*1000;

P_PCM_max = 1400*1000;
E_PCM_max = 5740*1000;

% Define efficiency values (charging/discharging)
eta_HP = 0.95;
eta_BAT = 0.90;
eta_PCM = 0.85;

% Market electricity prices (€/kWh)
market_price = [0.85, 0.65, 0.65, 0.65, 0.65, 0.65, 0.65, 0.85, 0.85, 0.85, 0.85, 0.85, ...
                0.85, 0.65, 0.65, 0.65, 0.65, 0.65, 0.65, 0.85, 0.85, 0.85, 0.85, 0.85];

% Load data: Total thermal loads, electrical loads, and PV generation
thermal_loads = [62750, 62750, 62750, 62750, 62750, 89092.97521, 608333.3452, 1032778.937, ...
                 1047916.678, 857735.549, 744064.0614, 763975.7698, 718980.7281, 719568.8824, ...
                 694402.2157, 720157.0366, 765235.4117, 745323.7033, 475871.9008, 601455.2342, ...
                 491009.6419, 296342.9752, 151926.3085, 32004.82094];

electrical_loads = [391735.7253, 395127.2883, 386838.7718, 400327.6008, 398289.0843, ...
                    485746.1617, 487656.9442, 515761.0326, 559945.916, 571927.4889, ...
                    544977.1977, 505042.4123, 497512.8491, 507461.4182, 539057.1804, ...
                    556814.9979, 548009.3217, 497718.7258, 417148.0062, 465631.2786, ...
                    512742.0297, 508746.0442, 510549.1936, 500701.4806];

pv_generation = [0, 0, 0, 0, 0, 0, 0, 1112.5, 287081.25, 1049418.75, 1648937.5, 1230287.5, ...
                 1358200, 931306.25, 1449737.5, 1487075, 1547712.5, 612237.5, 79631.25, ...
                 31.25, 0, 0, 0, 0];



% **Decision Variables** (Single `P_ESS`)
num_vars = 5 * T; % Includes P_HP, P_BAT, P_PCM, P_sell, P_buy
x = optimvar('x', num_vars, 'LowerBound', -inf);

% **Objective Function: Maximize Profit (Minimize Cost - Revenue)**
f = [-market_price, -market_price, -market_price, market_price, -market_price]'; % Revenue (-sell, +buy)

% **Equality Constraints**
Aeq = zeros(3*T, num_vars);
beq = zeros(3*T, 1);

for t = 1:T
    % **Power balance equation**
    Aeq(t, t) = eta_HP;   % Hydro Pump Power
    Aeq(t, T+t) = eta_BAT;  % Battery Power
    Aeq(t, 2*T+t) = eta_PCM;  % PCM Power
    Aeq(t, 3*T+t) = -1; % Selling to market
    Aeq(t, 4*T+t) = 1; % Buying from market
    beq(t) = electrical_loads(t) - pv_generation(t);
    
    % **SOC Constraints for Battery**
    if t > 1
        Aeq(T+t, T+t-1) = 1; % Previous SOC
        Aeq(T+t, t) = eta_BAT * (x(T+t) <= 0); % Charging (negative)
        Aeq(T+t, t) = -1/eta_BAT * (x(T+t) >= 0); % Discharging (positive)
        beq(T+t) = 0;
    end
    
    % **SOC Constraints for Hydro Pump**
    if t > 1
        Aeq(2*T+t, 2*T+t-1) = 1; % Previous SOC
        Aeq(2*T+t, t) = eta_HP * (x(2*T+t) <= 0); % Charging (negative)
        Aeq(2*T+t, t) = -1/eta_HP * (x(2*T+t) >= 0); % Discharging (positive)
        beq(2*T+t) = 0;
    end
end

% **SOC Limits (Ensure SOC stays within min/max bounds)**
SOC_HP_max = E_HP_max;
SOC_BAT_max = E_BAT_max;
SOC_PCM_max = E_PCM_max;

lb = -[P_HP_max * ones(1, T), P_BAT_max * ones(1, T), P_PCM_max * ones(1, T), zeros(1, T), zeros(1, T)]';
ub = [P_HP_max * ones(1, T), P_BAT_max * ones(1, T), P_PCM_max * ones(1, T), inf(1, T), inf(1, T)]';

% **SOC Initial and Final Constraints**
Aeq(3*T-2, T+1) = 1; Aeq(3*T-2, 2*T) = -1; beq(3*T-2) = 0;
Aeq(3*T-1, 2*T+1) = 1; Aeq(3*T-1, 3*T) = -1; beq(3*T-1) = 0;
Aeq(3*T, 3*T+1) = 1; Aeq(3*T, 4*T) = -1; beq(3*T) = 0;

% **Solve the Optimization Problem**
options = optimoptions('linprog', 'Algorithm', 'dual-simplex');
[x_opt, fval, exitflag] = linprog(f, [], [], Aeq, beq, lb, ub, options);

% **Check for Errors**
if exitflag ~= 1
    error('Optimization failed. Exit flag: %d', exitflag);
end

% **Extract Results (Single P_ESS)**
P_HP = x_opt(1:T);   % Hydro Pump (Charge: Negative, Discharge: Positive)
P_BAT = x_opt(T+1:2*T);  % Battery (Charge: Negative, Discharge: Positive)
P_PCM = x_opt(2*T+1:3*T);  % PCM (Charge: Negative, Discharge: Positive)
P_sell = x_opt(3*T+1:4*T); % Sold energy to market
P_buy = x_opt(4*T+1:5*T);  % Bought energy from market

% **Display Results in a Table**
disp(table((1:T)', P_HP, P_BAT, P_PCM, P_sell, P_buy, ...
    'VariableNames', {'Time', 'HP_Power', 'BAT_Power', 'PCM_Power', 'Sell', 'Buy'}));

% **Plot Dispatch Results**
figure;
subplot(3,1,1);
plot(1:T, P_HP, '-o', 'LineWidth', 1.5);
title('Hydro Pump Dispatch');
xlabel('Time (hours)'); ylabel('Power (kW)');
legend('Charge (-), Discharge (+)');

subplot(3,1,2);
plot(1:T, P_BAT, '-o', 'LineWidth', 1.5);
title('Battery Dispatch');
xlabel('Time (hours)'); ylabel('Power (kW)');
legend('Charge (-), Discharge (+)');

subplot(3,1,3);
plot(1:T, P_PCM, '-o', 'LineWidth', 1.5);
title('PCM Dispatch');
xlabel('Time (hours)'); ylabel('Power (kW)');
legend('Charge (-), Discharge (+)');
