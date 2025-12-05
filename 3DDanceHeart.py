import pygame, math, random
from pygame.locals import *

pygame.init()
WIDTH, HEIGHT = 800, 600
screen = pygame.display.set_mode((WIDTH, HEIGHT))
pygame.display.set_caption("3D粒子旋转爱心")
BLACK = (0, 0, 0)
PINKS = [(255, 105, 180), (255, 182, 193), (255, 20, 147), (255, 192, 203), (255, 100, 150)]
angle_y, pulse = 0, 0


def is_in_heart(x, y, z):
    """改进的心形判断函数，创建更标准的心形"""
    # 缩放坐标
    scale = 0.7
    x, y, z = x / scale, y / scale, z / scale

    # 使用更精确的心形方程
    # 心形表面方程: (x^2 + 9/4*y^2 + z^2 - 1)^3 - x^2*z^3 - 9/80*y^2*z^3 = 0
    t1 = (x * x + (9 * y * y) / 4 + z * z - 1)
    result = t1 ** 3 - x * x * z ** 3 - (9 * y * y * z ** 3) / 80

    # 增加厚度使心形更饱满
    return result <= 0.1 and result >= -0.2


class Particle:
    def __init__(self):
        self.reset()

    def reset(self):
        # 使用更好的采样方法生成心形内部粒子
        while True:
            x, y, z = [random.uniform(-1.5, 1.5) for _ in range(3)]
            if is_in_heart(x, y, z):
                break

        self.x, self.y, self.z = x * 12, y * 12, z * 12
        # 减小移动速度使粒子更稳定
        self.dx, self.dy, self.dz = [random.uniform(-0.01, 0.01) for _ in range(3)]
        self.color = random.choice(PINKS)
        self.size = random.uniform(1.5, 3.5)
        # 添加生命周期属性
        self.life = random.uniform(0.5, 1.0)

    def update(self, a, p):
        # 更新粒子位置
        self.x += self.dx
        self.y += self.dy
        self.z += self.dz

        # 如果粒子离开心形范围则重置
        if not is_in_heart(self.x / 12, self.y / 12, self.z / 12):
            self.reset()

        # 应用旋转矩阵（绕Y轴旋转）
        xr = self.x * math.cos(a) + self.z * math.sin(a)
        zr = -self.x * math.sin(a) + self.z * math.cos(a)

        # 投影到2D屏幕
        scale = 25 * p
        dist = 20
        xp = WIDTH // 2 + int(xr * scale)
        yp = HEIGHT // 2 - int(self.y * scale)

        # 深度计算影响大小和透明度
        depth = (zr + dist) / (2 * dist)
        size = max(2, int(self.size * depth * 1.8))

        # 根据深度调整颜色亮度
        brightness = min(1, max(0.3, depth * 1.8))
        col = tuple(min(255, max(0, int(c * brightness))) for c in self.color)

        return xp, yp, size, col

    def draw(self, s, a, p):
        x, y, size, col = self.update(a, p)

        # 只绘制在屏幕范围内的粒子
        if 0 <= x < WIDTH and 0 <= y < HEIGHT and size > 0:
            # 创建带透明度的表面
            surf = pygame.Surface((size * 2, size * 2), pygame.SRCALPHA)

            # 根据深度调整透明度
            alpha = int(255 * min(1, max(0.2, (size / 8))))
            pygame.draw.circle(surf, col + (alpha,), (size, size), size)
            s.blit(surf, (x - size, y - size))


# 创建更多粒子以获得更饱满的效果
particles = [Particle() for _ in range(5000)]
clock = pygame.time.Clock()
running = True

while running:
    for e in pygame.event.get():
        if e.type == QUIT:
            running = False

    # 控制旋转速度
    angle_y += 0.012

    # 创建更有节奏感的脉动效果
    time_factor = pygame.time.get_ticks() * 0.002
    pulse = (math.sin(time_factor) + 1) / 2 * 0.3 + 0.85

    # 清屏
    screen.fill(BLACK)

    # 绘制所有粒子（按Z坐标排序以实现正确的遮挡关系）
    sorted_particles = sorted(particles, key=lambda p: (
            -p.x * math.sin(angle_y) + p.z * math.cos(angle_y)  # Z坐标排序
    ))

    for p in sorted_particles:
        p.draw(screen, angle_y, pulse)

    pygame.display.flip()
    clock.tick(60)

pygame.quit()
